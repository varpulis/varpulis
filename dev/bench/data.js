window.BENCHMARK_DATA = {
  "lastUpdate": 1788173608178,
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
          "id": "a242a86b0d84c1cfc5101999a8b24f08309a5a97",
          "message": "fix: resolve P1 production readiness issues (#19-#22)\n\nBounded collections (#19): Add LRU eviction for PMC prediction/forecast\ncaches (hashlink::LruCache), PST arena compaction with BFS reachability,\ngraphlet cap with oldest-inactive eviction, and snapshot cap enforcement.\nPrevents OOM in long-running streams.\n\nCircuit breaker for sources (#20): Replace ad-hoc error counters in all 6\nconnector source loops (mqtt, kafka, nats + managed variants) with the\nexisting CircuitBreaker. Adds Display/Serialize on State, health report\nfields, and consistent backoff behavior across all connectors.\n\nConnector error-path tests (#21): 23 new tests covering stub NotAvailable\nreturns, database SQL injection validation, REST API header validation,\nmanaged registry errors, and json_to_event resource limit enforcement.\n\nCLI command handler tests (#22): Move resolve_imports to lib.rs for\ntestability, add simulate_from_source helper. 20 new tests covering VPL\nvalidation, config file loading, recursive import resolution with cycle\ndetection and depth limiting, simulation pipeline, and project config\ndiscovery.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-23T12:50:33+01:00",
          "tree_id": "ff79e296c8c3886fa2a7f2ce7ef821f70b165244",
          "url": "https://github.com/varpulis/varpulis/commit/a242a86b0d84c1cfc5101999a8b24f08309a5a97"
        },
        "date": 1771847790548,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33178,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3314500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 436410,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2204700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32156,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 384930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1945100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2132100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3857600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1487600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1674900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 112830000,
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
            "value": 3321700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16654000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34267000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33162000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19286000,
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
          "id": "a4b4adc230b76b1412df60a48efc04037c09d093",
          "message": "fix: resolve P0 production readiness issues (#15-#18)\n\n- Add configurable CORS origins to API routes (cli + cluster)\n- Fix Dockerfile health check and exposed ports (8080 -> 9000)\n- Add try_get_node_from_ref to ZDD UniqueTable for safe access\n- Update all test call sites with new cors_origins parameter\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-23T12:58:32+01:00",
          "tree_id": "143f18e8f088fd1a3dc401cdd784b69d3ce6662e",
          "url": "https://github.com/varpulis/varpulis/commit/a4b4adc230b76b1412df60a48efc04037c09d093"
        },
        "date": 1771848262843,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 332370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3272800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39363,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 436880,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2207800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32219,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1955700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2085799,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3885800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1475600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1688500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 111690000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1761300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3233500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16012000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32560000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33206000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19256000,
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
          "id": "2c744f54402f5fc93e24bab3ad08b228021f6b73",
          "message": "fix: resolve P2/P3 production readiness issues (#23-#30)\n\n- Remove 21 dead code items across 12 files (#23)\n- Update STATUS.md known limitations (#24)\n- Enhance DLQ with configurable path/size, rotation, Prometheus\n  counter, and REST API endpoints for read/replay/clear (#25)\n- Rewrite Grafana overview dashboard with 25 panels, template\n  variables, latency heatmap, cluster health row (#26)\n- Add OpenTelemetry tracing behind `otel` feature flag with OTLP\n  exporter, W3C traceparent propagation in NATS (#27)\n- Add backpressure signaling: --max-queue-depth CLI flag, HTTP 429\n  with Retry-After, queue_pressure_ratio gauge, alert rule (#28)\n- Add capacity planning guide with sizing tables (#29)\n- Add TLS certificate management documentation (#30)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-23T15:42:01+01:00",
          "tree_id": "74947e3cb2e6e9eaf95d1a79e64dfac523eb0bdf",
          "url": "https://github.com/varpulis/varpulis/commit/2c744f54402f5fc93e24bab3ad08b228021f6b73"
        },
        "date": 1771858071586,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33066,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 327620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3250400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 437300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2193600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32409,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 383030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1930100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2123900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3916900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1476100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1675100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 109660000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1737300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3289000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16437999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33667000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32997000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19287000,
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
          "id": "2108d8d2761ee4d553190ae5aff773506704371b",
          "message": "release: v0.4.0 — rewrite README, add changelog, bump version\n\n- Rewrite README: remove adversarial competitor comparisons, lead with\n  \"What is Varpulis?\" and use cases, standalone performance framing\n- Add CHANGELOG entries for v0.3.0 (PST forecasting, ONNX, NATS, MCP,\n  HA cluster, security hardening) and v0.4.0 (production readiness audit\n  18/18 complete, DLQ API, OpenTelemetry, backpressure)\n- Bump version 0.3.0 → 0.4.0 in Cargo.toml, package.json, STATUS.md\n- Update roadmap: check off Phase 3/4/5 completed items\n- Update test count badge (3776 → 3899)\n- Add .forecast(), NATS, OTel, backpressure, MCP to README features\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-23T16:50:21+01:00",
          "tree_id": "582f30b3f9b9d237f0b08ff3ccdf07dc8f7f30f3",
          "url": "https://github.com/varpulis/varpulis/commit/2108d8d2761ee4d553190ae5aff773506704371b"
        },
        "date": 1771862183318,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34178,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 334820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3332900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 443200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2186300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33431,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 399560,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2022500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2117400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3910000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1486000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1721700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115750000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1958800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3272400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16509000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32863999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32987000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19824000,
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
          "id": "461cb0c7536aa25fd954e81a71b797c73e9e6390",
          "message": "docs(readme): add \"Why Varpulis?\" value proposition section\n\nLead with the problem (patterns buried in event firehose, detected too\nlate) and why Varpulis solves it (concise DSL, sub-ms performance,\npredictive forecasting, deploy-anywhere). Rename \"What is Varpulis?\"\nto separate Why/What sections.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-23T16:56:38+01:00",
          "tree_id": "e98158bbc2f4ee92d941822bcde996a46c3d5ad0",
          "url": "https://github.com/varpulis/varpulis/commit/461cb0c7536aa25fd954e81a71b797c73e9e6390"
        },
        "date": 1771862565308,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33442,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 330950,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3315500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40015,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 426840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2181700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 389070,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1954800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2112800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3943700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1473100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1686300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 117060000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1784900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3246900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16285000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32567000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33148000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19157000,
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
          "id": "1724fac989baedc759a2fad850a47e2cd8563588",
          "message": "feat(validate): add strict VPL semantic validation\n\n- Fix FollowedBy/Not event validation: `stream S = A -> B` now validates\n  both A and B (previously B was silently ignored)\n- Promote undeclared references from warning to error (W030→E033, W031→E034)\n- Add connector type validation (E008) against known types list\n- Add required connector parameter infrastructure (E009)\n- Add no-output warning (W033) for streams missing .emit/.to/.print/.log\n- Add field reference validation (W034) with alias-to-event mapping\n- Fix clippy is_some_and lint in LSP completion\n- Update 115+ tests for new error codes and strictness\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T00:55:12+01:00",
          "tree_id": "4e0f66bfb4fd3f2a343cd4827e66ab98e6541ce3",
          "url": "https://github.com/varpulis/varpulis/commit/1724fac989baedc759a2fad850a47e2cd8563588"
        },
        "date": 1771891315708,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33470,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331220,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3335600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40597,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 440940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2204400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33222,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 401380,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2010500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2131800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3879000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1491900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1726800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 116150000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1739100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3345300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16777000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33623000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33974000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19365000,
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
          "id": "5cec18663a98be47dc8c45a44382dffbc281c513",
          "message": "fix(validate): remove W033 no-output warning, add connector connection params\n\n- Remove W033 warning that incorrectly flagged intermediate streams\n  without explicit output operations (streams without .emit()/.to() are valid)\n- Add connection-level params to connector schemas: host/port/url for MQTT,\n  brokers for Kafka, url for NATS/HTTP\n- Change Kafka group_id from Source-only to Both context (valid in declarations)\n- Fix tests across MCP, LSP, runtime that used undeclared event types\n  (now caught by E033 strict validation from previous commit)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T01:19:24+01:00",
          "tree_id": "1124847905f01428677680029fc82a6aa0b0c0fa",
          "url": "https://github.com/varpulis/varpulis/commit/5cec18663a98be47dc8c45a44382dffbc281c513"
        },
        "date": 1771892732555,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33726,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 333680,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3360800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41094,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 444290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2212800,
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
            "value": 389560,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1991700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2086300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3827200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1461500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1670300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113040000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1760800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3495300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17225000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34387000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33534999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19559000,
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
          "id": "7ab3cd336c89095c03594afa06cfac0abf6c4c32",
          "message": "fix(validate): correct connector param schemas (brokers=StrArray, http=base_url)\n\n- Change Kafka `brokers` param type from Str to StrArray (it's an array of strings)\n- Replace HTTP connector params: remove url/topic/method, add base_url as the\n  only connection parameter\n- Add ParamType::StrArray variant with proper validation and LSP completion snippet\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T01:44:17+01:00",
          "tree_id": "eef3a8e1f13e206d5566520e562cbd5f486d5f9c",
          "url": "https://github.com/varpulis/varpulis/commit/7ab3cd336c89095c03594afa06cfac0abf6c4c32"
        },
        "date": 1771894224144,
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
            "value": 330330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3312100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40217,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 436650,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2172500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32509,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 387230,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1953800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2135600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4039900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1498700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1675500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 111040000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1792500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3263700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16729000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32827000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32866000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19457000,
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
          "id": "86e67714ce6e64ac2b80a928f55141305a97dac6",
          "message": "feat(lsp): per-op diagnostic spans, merge/log/print support, unknown op errors\n\n- Add op_spans field to StreamDecl AST for per-operation source spans\n- Parser captures pest spans for each stream operation\n- Validator uses per-op spans so diagnostics highlight the specific\n  operation instead of the entire stream declaration line\n- Add .log() and .print() to LSP stream operation completions\n- Add merge source handling in validator (alias_to_event mapping) and\n  LSP field completions (inline stream extraction, multi-line support)\n- Add W061 warning for bare identifiers in .where() conditions\n- Improve unknown stream operation parse error: concise message with\n  hint listing valid operations instead of 30+ raw rule names\n- Add '(' as completion trigger character for context-aware completions\n- Add join source handling in validator\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T11:30:55+01:00",
          "tree_id": "a42476dbf3394148946eb6df77b295b4a04bc0da",
          "url": "https://github.com/varpulis/varpulis/commit/86e67714ce6e64ac2b80a928f55141305a97dac6"
        },
        "date": 1771929415112,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3303000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40778,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 460330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2223300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32975,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1963100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2133600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3902700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1485300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1695200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115390000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1770500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3866800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 19223000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 37907000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33360999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20741000,
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
          "id": "818eb76adf4e6de1ce4e3c486fc5b3467962c0b4",
          "message": "feat: add cloud SaaS infrastructure (OAuth, DB, billing, playground)\n\nPhase 1: WASM parser crate, playground backend API with ephemeral sessions,\nplayground UI (3-panel layout with examples), and marketing landing page.\n\nPhase 2: Event generator library (fraud/IoT/trading schemas), fraud detection\nand IoT monitoring Docker demos with Grafana dashboards.\n\nPhase 3: GitHub OAuth login flow with JWT sessions, PostgreSQL database layer\n(users, orgs, API keys, pipelines, usage tracking), Stripe billing integration\nwith tier management (Free/Pro/Enterprise) and usage metering.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T13:54:32+01:00",
          "tree_id": "79214d658b9082b50856ac22bdf600533941b8c0",
          "url": "https://github.com/varpulis/varpulis/commit/818eb76adf4e6de1ce4e3c486fc5b3467962c0b4"
        },
        "date": 1771938024820,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34501,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 337500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3334000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41190,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 443810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2219800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33074,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 398770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1993000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2287200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3895300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1458600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1698100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 116960000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1806900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3338900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16830000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33891000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33660000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19578000,
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
          "id": "2ec578b7a36bfc05363895cf62554956800be005",
          "message": "feat: add distribution infrastructure (Homebrew, GitHub Action, crates.io publish)\n\n- GitHub Actions marketplace action for VPL file validation in CI\n- Homebrew formula for macOS/Linux binary installation\n- crates.io publish workflow with dependency-ordered sequential publishing\n- Self-test workflow for VPL check action on examples/ and demos/\n- CHANGELOG updated with Phase 3 & 4 entries (OAuth, DB, billing, playground, datagen, WASM)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T14:26:31+01:00",
          "tree_id": "f98f41851304792f8ac833da3bf3f305e92930af",
          "url": "https://github.com/varpulis/varpulis/commit/2ec578b7a36bfc05363895cf62554956800be005"
        },
        "date": 1771939935858,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33694,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 332540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3312300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40830,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 448370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2183100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33186,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 393860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2014300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2130800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3928800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1480700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1681700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 114580000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1773800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3283600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16834000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32887000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33139000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19451000,
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
          "id": "7505eceec14ca516e3d1adb439bd537584bdc150",
          "message": "feat(saas): wire end-to-end revenue pipeline (OAuth→DB→Stripe→billing)\n\nConnect the SaaS scaffolding into a working revenue pipeline:\n\n- Wire PostgreSQL into CLI behind `saas` feature flag with pool + migrations\n- OAuth callback upserts user/org in DB, enriches JWT with user_id/org_id\n- Real Stripe API calls for checkout sessions and customer portal (reqwest)\n- Stripe webhook handler with HMAC-SHA256 signature verification\n- Org + API key CRUD endpoints (generate vpl_xxx keys, SHA-256 hashed)\n- Usage metering: in-memory buffer flushed to DB every 60s\n- Frontend: auth guard, org store, pricing page, API key management in settings\n- Docker Compose: PostgreSQL service, env vars for OAuth/Stripe/DB\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T15:26:43+01:00",
          "tree_id": "82cb23dded30116d8e126885a7c3314011d7e355",
          "url": "https://github.com/varpulis/varpulis/commit/7505eceec14ca516e3d1adb439bd537584bdc150"
        },
        "date": 1771943564159,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34521,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 347570,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3483200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41352,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 406500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2262500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33552,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 404680,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2021100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2202400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4151500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1543800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1783000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 125160000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1831500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3432700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17276000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33593000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 34066000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 21394000,
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
          "id": "6913dd257287218313ebc7dfb5500dc787a0b8bb",
          "message": "feat: add Phase 6 advanced infrastructure (connectors, concurrency, GPU, federation)\n\n- Pulsar connector (source/sink, feature-gated)\n- Redis Streams connector (XREADGROUP/XADD, feature-gated)\n- .concurrent() operator with Rayon thread pool parallelization\n- GPU-accelerated .score() inference (CUDA/TensorRT, batch mode)\n- Multi-region federation (coordinator, routing, API, CLI)\n- .gitignore: fuzz corpus/target, benchmark results, test results, tree-sitter artifacts\n- GitHub community files (CODEOWNERS, PR template, issue templates, dependabot)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T16:40:53+01:00",
          "tree_id": "ff5ff4875a14d33cc54a4ca0dea9607e64a932fe",
          "url": "https://github.com/varpulis/varpulis/commit/6913dd257287218313ebc7dfb5500dc787a0b8bb"
        },
        "date": 1771948033151,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 35345,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 346140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3475100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41474,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 460540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2289700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33908,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 417030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2080200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2181700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4109499,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1525600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1780900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113390000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1815900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3439800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17622000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 35070000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 35329000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20772000,
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
          "id": "c58d2c374e870214b86ad2ad50d4b47945ed3f84",
          "message": "fix(ci): redis Value API compat, federation handler args, pulsar protoc, deny advisory\n\n- redis: use Value::Bulk/Data instead of Array/BulkString (redis 0.25 API)\n- federation: add missing _auth parameter to warp handler functions\n- ci: install protobuf-compiler for pulsar feature flag job\n- deny.toml: allow RUSTSEC-2025-0052 (async-std, transitive via pulsar)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T17:35:36+01:00",
          "tree_id": "38d92a507474f78a51c9e892f9e5434e60be5133",
          "url": "https://github.com/varpulis/varpulis/commit/c58d2c374e870214b86ad2ad50d4b47945ed3f84"
        },
        "date": 1771952035384,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36695,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 369020,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3728500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 47829,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 511490,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2581900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 35943,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 418320,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2131200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2471700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4812200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1663600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1964500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 145450000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1959800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3850500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 20193000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 40558000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 39268000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 23816000,
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
          "id": "bfa62e8e574c3b55d63f524b4a010d3064d47e99",
          "message": "fix: pulsar Payload API, bump web-ui deps to latest majors\n\n- pulsar: access msg.payload.data directly instead of matching as Result\n- web-ui: bump vite 7, vuetify 4, echarts 6, vue-echarts 8, vue-router 5,\n  pinia 3, @vitejs/plugin-vue 6, @types/node 25\n- vuetify 4: replace item.raw.field with item.field in select/autocomplete slots\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T18:05:09+01:00",
          "tree_id": "630146ca3f4c335d25d6039ee65308a29af49cae",
          "url": "https://github.com/varpulis/varpulis/commit/bfa62e8e574c3b55d63f524b4a010d3064d47e99"
        },
        "date": 1771953063987,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33568,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 338250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3344700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41036,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 442460,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2212100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33127,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 394720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2009599,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2145800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3915200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1465900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1705200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115450000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1738000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3300200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16671000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33205000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33975000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19517000,
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
          "id": "0c3a00519d99b7c950f0f938732754263c8449a7",
          "message": "feat(saas): add audit logging, fix SaaS deployment stack\n\n- Add structured audit logging module (audit.rs) with JSON-lines output,\n  in-memory recent buffer, and GET /api/v1/audit endpoint\n- Rewrite docker-compose.saas.yml with PostgreSQL, Caddy reverse proxy,\n  Web UI, and all required environment variables for OAuth/Stripe/JWT\n- Add Caddyfile.saas for API/auth/WebSocket routing to backend\n- Add .env.example documenting all SaaS configuration variables\n- Update CHANGELOG with audit logging, connectors, and deployment entries\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T18:14:42+01:00",
          "tree_id": "cb220abf4ce47fea648ecc75f30cb851438cc9db",
          "url": "https://github.com/varpulis/varpulis/commit/0c3a00519d99b7c950f0f938732754263c8449a7"
        },
        "date": 1771953634536,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 35214,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 352200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3478300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41210,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 447140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2246800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33694,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 401990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2024800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2215800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4011700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1522800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1773700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113450000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1769600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3328800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16600000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33308999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 34755000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19588000,
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
          "id": "817ed5407bd76ae6c6f6e431d6c5020ca42c532a",
          "message": "fix(pulsar): remove unused imports and dead code field\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T18:18:10+01:00",
          "tree_id": "c919c8f22912a3be59667babb5f16f7bb05a7f4e",
          "url": "https://github.com/varpulis/varpulis/commit/817ed5407bd76ae6c6f6e431d6c5020ca42c532a"
        },
        "date": 1771953842226,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34523,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 340900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3396800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40158,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 425170,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2182200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33305,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 406630,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2035600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2118300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3913700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1477100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1671700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 117990000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1790300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3279000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16335999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33098000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33799000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19411000,
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
          "id": "bb8916cd6646fc3002090aaece7242c8f67eb627",
          "message": "chore: bump deps (opentelemetry 0.31, prometheus 0.14, rmcp 0.16, toml 1) and CI actions\n\nRust crates:\n- opentelemetry/opentelemetry_sdk/opentelemetry-otlp: 0.27 → 0.31\n- tracing-opentelemetry: 0.28 → 0.32\n- prometheus: 0.13 → 0.14\n- rmcp: 0.15 → 0.16\n- toml: 0.8 → 1.0\n\nGitHub Actions:\n- actions/checkout: v4 → v6\n- actions/cache: v4 → v5\n- actions/download-artifact: v4 → v7\n- codecov/codecov-action: v4 → v5\n- softprops/action-gh-release: v1 → v2\n\nAlso: wire billing_state through api_routes, fix missing 4th arg in test\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T19:34:06+01:00",
          "tree_id": "4b3857cdfd6f74bf5ec778133ad9031e64034b7a",
          "url": "https://github.com/varpulis/varpulis/commit/bb8916cd6646fc3002090aaece7242c8f67eb627"
        },
        "date": 1771958469996,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33978,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3271500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39792,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 442470,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2184200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32529,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 386970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1952700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2114100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3875000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1486900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1664600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115670000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1779100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3428000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16443999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32851999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32704000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20111000,
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
          "id": "36973a835bd663b6be5dacc85fe653920b9901b3",
          "message": "feat(saas): add usage limit enforcement, audit integration, prod config\n\n- Add tier-based usage limit checking (Free 10K/mo, Pro 10M/mo, Enterprise unlimited)\n- Return 429 Too Many Requests when limit exceeded with upgrade instructions\n- Wire billing state into API inject handlers for usage tracking\n- Add audit logging to OAuth (login/logout) and billing (checkout/webhook/tier changes)\n- Create production Docker Compose overlay with resource limits and pg backup\n- Add Stripe setup guide and automation script\n- Move audit logger initialization before OAuth/billing for proper wiring\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-24T23:27:41+01:00",
          "tree_id": "ef7c496dc2b17b9d7bb756db5c1a961a5d9fcedd",
          "url": "https://github.com/varpulis/varpulis/commit/36973a835bd663b6be5dacc85fe653920b9901b3"
        },
        "date": 1771972430614,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33577,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331040,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3356700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39901,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 443710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2177900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32647,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 392940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2007400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2165700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3991600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1468800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1668400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113550000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1772700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3264100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16361999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32704000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33018000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19377000,
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
          "id": "0d736d62657ed9a1c59e52adbb5e8b86012c2283",
          "message": "feat: Phase 6 E2E tests, demo scenarios, and Docker integration infra\n\nAdd comprehensive test coverage for Phase 6 features (.concurrent(),\n.score() GPU/batch, federation, Pulsar, Redis Streams) and three new\ninteractive demo scenarios for the web UI.\n\nTests:\n- 8 concurrent runtime E2E tests (partition key ordering, large batch, sequences)\n- 6 score batch/GPU config tests (infer_batch, GpuConfig, VPL parsing)\n- 10 federation tests (health transitions, catalog filtering, routing wildcards)\n- 10 Pulsar/Redis connector config + stub tests\n- 3 Pulsar + 5 Redis Streams Docker integration tests (#[ignore])\n- Docker Compose + Dockerfiles + runner scripts for Pulsar and Redis E2E\n\nWeb UI:\n- 3 new demo scenarios: IoT Concurrent, AI Fraud Scoring, Multi-Region Federation\n- 3 new demo views with alert panels\n- Landing page: \"11 Connectors\", 4 Phase 6 feature cards\n- Fix Vuetify 4: remove deprecated offset-y from ExampleSelector\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T00:56:08+01:00",
          "tree_id": "d1bcc3b1ee203e554a9b23f4ce6c91d1d0b23d19",
          "url": "https://github.com/varpulis/varpulis/commit/0d736d62657ed9a1c59e52adbb5e8b86012c2283"
        },
        "date": 1771977723813,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33624,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 333610,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3276000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40473,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 444830,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2222500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32718,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 396040,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2035000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2150100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3895800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1467100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1685200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 116040000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1739200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3326700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16945000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33249000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32985000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19470000,
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
          "id": "0e6c8f2cdcac9619937012d432064509279b07a4",
          "message": "fix(tests): redis integration tests compile under `redis` feature flag\n\n- RedisSink::new() is async when redis feature enabled — add .await\n- Use &*event.event_type instead of .as_str() (unstable str_as_str)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T01:03:23+01:00",
          "tree_id": "fe0b0532d4412ff6a9ee97576ab7c63ad7f0eca6",
          "url": "https://github.com/varpulis/varpulis/commit/0e6c8f2cdcac9619937012d432064509279b07a4"
        },
        "date": 1771978154734,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33436,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 328430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3253400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39534,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 437320,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2138200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32362,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 384670,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1956600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2111100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3915300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1457900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1667500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113300000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1753700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3229300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16407000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32820999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32592000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19161000,
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
          "id": "774d206ff697eba00e47c916b903be4e26299e05",
          "message": "fix(tests): resolve CI failures for onnx, pulsar, gpu feature flags\n\n- pulsar_integration.rs: use &*event.event_type (unstable str_as_str)\n- score_onnx_tests.rs: use Program.statements with Stmt::StreamDecl\n  instead of non-existent Program.streams field\n- score_onnx_tests.rs: vec![] → array literal (clippy::useless_vec)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T01:21:34+01:00",
          "tree_id": "7558b30f028eeb6c7db19486d963b81bb8590304",
          "url": "https://github.com/varpulis/varpulis/commit/774d206ff697eba00e47c916b903be4e26299e05"
        },
        "date": 1771979302369,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33773,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 335430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3345200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40252,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 441200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2214100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32613,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 387490,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1975400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2147800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3906900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1487700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1716900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115510000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2011400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3295400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16457000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33286000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33149000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19447000,
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
          "id": "168732290c4f0942344ed8d955675f10cf3ae96b",
          "message": "fix(web-ui): update version display from v0.2.0 to v0.4.0\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T09:16:40+01:00",
          "tree_id": "0b6cf388905b889aef67cbe3e03b0e6f9f1d4247",
          "url": "https://github.com/varpulis/varpulis/commit/168732290c4f0942344ed8d955675f10cf3ae96b"
        },
        "date": 1772007750989,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33785,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 334540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3330700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 446140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2195500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32631,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 386300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1970800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2119800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3893500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1460900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1666700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 114790000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1744200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3305400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16620000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32933999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33505000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19188000,
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
          "id": "f6230eb955aaab9c9a587de22a4c4920086dfdb8",
          "message": "fix: demo scenarios, model upload, AI assistant reliability, Docker onnx\n\n- Add event declarations to multi-region and AI fraud scoring VPL demos\n- Use bundled fraud_scorer.onnx model in AI scoring demo instead of missing fraud_nn.onnx\n- Enable onnx feature in Docker worker builds, bundle ONNX models in image\n- Add file upload support to model registry (base64 encode/decode + disk write)\n- Fix AI assistant timeouts with 90s reqwest timeout and proper HTTP status codes\n- Add base64 dependency for model file decoding\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T10:24:54+01:00",
          "tree_id": "28dcfa67a5549fb4c400ddd73be0ad66b049a848",
          "url": "https://github.com/varpulis/varpulis/commit/f6230eb955aaab9c9a587de22a4c4920086dfdb8"
        },
        "date": 1772011850777,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 35256,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 347010,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3499500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 460970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2295100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 34424,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 414770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2116900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2140800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4111100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1538100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1806100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 112630000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1843200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3459400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17446000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 35234000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 35140000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20454000,
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
          "id": "4623c34a55fa6cd5f0903f362ef6a1b59ebbd74a",
          "message": "fix(demos): IoT concurrent VPL uses `or` not `||`, add demo smoke tests\n\nVPL uses keyword `or`/`and` for logical operators, not `||`/`&&`.\nAdd smoke tests for all three Phase 6 demos verifying parse, load,\nand event processing end-to-end.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T12:10:02+01:00",
          "tree_id": "31c80e208f74e9434f92eb4cb82d0570f50cf7f5",
          "url": "https://github.com/varpulis/varpulis/commit/4623c34a55fa6cd5f0903f362ef6a1b59ebbd74a"
        },
        "date": 1772018154609,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33477,
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
            "value": 3297900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40118,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 450460,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2218700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32564,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 391770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1974700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2190000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3911200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1461200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1684400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113790000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1731600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3378100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16816000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33786000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32970000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19298000,
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
          "id": "c06436f93f637bf004a60ae9d7a49ba5c461a3d7",
          "message": "fix: remove 30s timeout wrapping entire server lifetime\n\nThe tokio::time::timeout(30s, server) was wrapping the entire warp\nserver future, not just the graceful shutdown drain period. This caused\ncoordinators and workers to self-exit after exactly 30 seconds of normal\noperation, creating a restart loop in Docker deployments.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T15:42:57+01:00",
          "tree_id": "192763dfe536bee5025f101fe26b8839584a254c",
          "url": "https://github.com/varpulis/varpulis/commit/c06436f93f637bf004a60ae9d7a49ba5c461a3d7"
        },
        "date": 1772030936016,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 37929,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 362330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3629900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 43949,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 463860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2543100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 34093,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 406730,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2064200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2248600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4036200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1839300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1894300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 114830000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1787700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3364800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16998000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33137000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33329000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19619000,
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
          "id": "5c36d4e83f572c5468dc85c02218989ebcb2c363",
          "message": "fix(docker): install ONNX Runtime library for .score() operator\n\nThe ort crate uses load-dynamic to dlopen libonnxruntime.so at runtime.\nThe runtime stage was missing this library, causing .score() pipelines\nto fail with \"dlopen failed\". Download and install ONNX Runtime 1.22.0.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T17:05:09+01:00",
          "tree_id": "3c63ec9ab036cdc8ea020241128b2c698388d9ef",
          "url": "https://github.com/varpulis/varpulis/commit/5c36d4e83f572c5468dc85c02218989ebcb2c363"
        },
        "date": 1772035886635,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33444,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 333360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3340300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40107,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 438130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2195100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32197,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 388450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1955700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2137300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4058700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1474900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1671400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 116850000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1755400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3288500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16484000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33051000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32649000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19157000,
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
          "id": "b4022e8eebad3555824ee8d5f57594a4c98328be",
          "message": "fix(docker): use ONNX Runtime 1.23.0 (required by ort 2.0.0-rc.11)\n\nort 2.0.0-rc.11 requires ONNX Runtime >= 1.23.x. Bump from 1.22.0.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T18:38:04+01:00",
          "tree_id": "c0071b29dcd11535362ae35b9dabc42bd40eab08",
          "url": "https://github.com/varpulis/varpulis/commit/b4022e8eebad3555824ee8d5f57594a4c98328be"
        },
        "date": 1772041426909,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33437,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 340180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3416700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40444,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 442780,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2208000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32719,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 394450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1985500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2162000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3922300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1471600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1704300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115260000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1767900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3253500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16206000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32704000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 34186000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19867000,
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
          "id": "84c3ea24c944bcf209e905771244725bd71a1048",
          "message": "fix(metrics): filter stale worker metrics from API\n\nThree fixes for stale metrics after worker restarts or pipeline migrations:\n\n1. get_cluster_metrics() cross-references with actual placements — only\n   returns metrics for pipelines currently placed on that worker\n2. deregister_worker() clears worker_metrics for the removed worker\n3. migrate_pipeline() removes source worker's metrics for the migrated\n   pipeline\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T19:13:32+01:00",
          "tree_id": "351e69a5b445b0e8ce4a9295e3eccd2a4d314ab1",
          "url": "https://github.com/varpulis/varpulis/commit/84c3ea24c944bcf209e905771244725bd71a1048"
        },
        "date": 1772043589672,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33509,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 333600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3346600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40286,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 449800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2204100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32569,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 389940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1989600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 1886600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3927500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1498100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1696700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 116780000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1770900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3350100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16856000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34820000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33372999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19322000,
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
          "id": "a5cf31e59719539b93d28481782c7b815d08198b",
          "message": "fix(caddy): remove /metrics proxy that conflicts with SPA route\n\nThe /metrics Caddy route was proxying to coordinator Prometheus endpoints,\noverriding the Vue SPA's /metrics page. On refresh, users saw raw\nPrometheus text instead of the web UI. Prometheus scrapes workers\ndirectly on port 9090 so this proxy was unnecessary.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T19:34:59+01:00",
          "tree_id": "bb5e119d2709742c33b6da7037efb85d4896d947",
          "url": "https://github.com/varpulis/varpulis/commit/a5cf31e59719539b93d28481782c7b815d08198b"
        },
        "date": 1772044869160,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36653,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 370110,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3734600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48225,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 508320,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2508400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 35476,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 415190,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2148600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2456900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4872100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1689300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1942400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 145370000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1973800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3861200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 20555000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 43072000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 40887000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 24201000,
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
          "id": "f0e45944b0ed72337758997ad953ea4c171e6635",
          "message": "fix(demo): all workers register with coordinator-1 for metrics\n\nWorker heartbeat metrics are stored in-memory on the coordinator they\nregister with. When workers were split across 3 coordinators, the\nmetrics API on any single coordinator only saw a subset of workers.\nPoint all workers at coordinator-1 so all metrics are in one place.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-25T20:05:16+01:00",
          "tree_id": "348b936bbb284a063535085610f790d34276d46a",
          "url": "https://github.com/varpulis/varpulis/commit/f0e45944b0ed72337758997ad953ea4c171e6635"
        },
        "date": 1772046713803,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36744,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 366180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3766800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48090,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 507720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2605100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 36022,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 426260,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2174700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2449100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4844200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1682400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1949400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 145200000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1975200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3853400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 20547000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 41369000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 38964000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 24008000,
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
          "id": "14866418487042827bc1373f13930b27c68601b5",
          "message": "feat(ws): relay worker output events through coordinator WebSocket\n\nWorkers now forward output events to the coordinator's internal\nendpoint, which broadcasts them to connected WebSocket clients.\nThis fixes the live event stream showing no events in the demo UI.\n\nAlso adds EventFlux comparison benchmark scaffolding.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T01:37:44+01:00",
          "tree_id": "75b7bb1e2b5e9ef48d09c23926443ec8798b9521",
          "url": "https://github.com/varpulis/varpulis/commit/14866418487042827bc1373f13930b27c68601b5"
        },
        "date": 1772066632871,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36859,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 366030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3877300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48005,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 511940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2560800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 35766,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 418530,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2160800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2483100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4966600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1662000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1958700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 149010000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1972400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4010300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 21126000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 41957000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 38867000,
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
          "id": "941a87736a8a5cfdc281747335b1b79c3aaaeab9",
          "message": "feat: add percentile aggregations, outer joins, encryption, OIDC, and CDC connector\n\nImplement 5 critical CEP gaps identified in market comparison:\n\n- G1: Percentile aggregations (median, p50, p95, p99, percentile(field, q))\n- G6: Outer joins (left_join, right_join, full_join) with null emission\n- G4: Encryption at rest via EncryptedStateStore<S> with AES-256-GCM\n- G3: SSO/OIDC via AuthProvider trait and generic OidcProvider\n- G2: PostgreSQL CDC connector via logical replication WAL polling\n\n4,011 tests pass. Documentation updated across builtins, connectors,\nwindows-aggregations, state-management, and CHANGELOG.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T12:16:36+01:00",
          "tree_id": "04f96cd5fcf11a2f05b942baf085551f49029ef4",
          "url": "https://github.com/varpulis/varpulis/commit/941a87736a8a5cfdc281747335b1b79c3aaaeab9"
        },
        "date": 1772104969691,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36864,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 365280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3720900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48331,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 510420,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2565300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 35998,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 419960,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2174600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2491800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4887900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1700300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2015499,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 145300000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1995600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3896100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 20544000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 41778000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 40014000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 23715000,
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
          "id": "15109f8b898ae3e9969e2b2b6b64f63af69711de",
          "message": "chore: remove accidental target/ from benchmark, add .gitignore\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T12:26:38+01:00",
          "tree_id": "d19104e365593725d0f827b602daf45bb7e46ceb",
          "url": "https://github.com/varpulis/varpulis/commit/15109f8b898ae3e9969e2b2b6b64f63af69711de"
        },
        "date": 1772106107540,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 39503,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 333510,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3339400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41403,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 448350,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2293600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32868,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 391730,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1992000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2161100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4157600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1469800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1715600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 118210000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1898100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3297600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16756000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33080000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 37085000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20671000,
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
          "id": "0feec1295151439798f8ccda38dd9ec68e0e57de",
          "message": "feat: sync LSP, tree-sitter, and TextMate grammars with new language features\n\nUpdate developer tooling to match the new aggregation functions, outer\njoins, and CDC connector added in the previous commit:\n\n- LSP: completions + hover for median/p50/p95/p99/percentile,\n  left_join/right_join/full_join, and postgres_cdc connector\n- builtins.rs: register new aggregates and connector type for validation\n- tree-sitter grammar: left_join/right_join/full_join source rules,\n  postgres_cdc connector type\n- TextMate grammar: syntax highlighting for new aggregations, join\n  keywords, and stream operations\n- CI: add cdc, encryption, oidc to feature-flags test matrix\n- Fix 3 tests that used now-valid function names as \"unknown\" examples\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T12:39:57+01:00",
          "tree_id": "0bf608565f3928214a36953836427fe19e5d35aa",
          "url": "https://github.com/varpulis/varpulis/commit/0feec1295151439798f8ccda38dd9ec68e0e57de"
        },
        "date": 1772106411946,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33762,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 332930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3290400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40706,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 442790,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2210300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33028,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1943600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2170600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3990700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1511000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1683000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 119650000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1770700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3311100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16570000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33005000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32796000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19138000,
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
          "id": "20adff6948a4ef380cae09445b6f17d25a13a28d",
          "message": "feat: harden output event relay, add GHCR CI, Kafka SASL, and install script\n\nRelay hardening:\n- Add RelayMetrics (forwarded/dropped/errors counters) to websocket.rs\n- Retry with exponential backoff (3 attempts) for worker→coordinator relay\n- Health-check gating after 5 consecutive failures (probe /health every 5s)\n- Increase worker broadcast buffer 100→10,000 (100ms slack at 100K events/s)\n- Add x-api-key auth to coordinator internal output-events endpoint\n- Expose relay metrics on worker /health endpoint\n- Replace all silent `let _ = broadcast.send()` with logged metrics\n\nCI & deployment:\n- Add .github/workflows/docker.yml — builds and pushes to GHCR on every main push\n- Add deploy/demo/docker-compose.prod.yml — compose override using GHCR images\n- Add deploy/demo/install.sh — curl-pipe-bash installer for any server\n- Add deploy/demo/deploy-pull.sh — quick pull-based update script\n- Add deploy/demo/README.md — comprehensive quick-start guide\n\nKafka SASL/SCRAM support:\n- Add VPL underscore→rdkafka dot-notation mappings for security_protocol,\n  sasl_mechanism, sasl_username, sasl_password, ssl_ca_location, etc.\n\nTesting & docs:\n- 5 new relay unit tests (all pass)\n- Local cluster E2E test script (tests/local-cluster/run.sh)\n- Architecture doc (docs/architecture/output-event-relay.md)\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T14:48:03+01:00",
          "tree_id": "d288566c9623248348e18266202d203e5fbfaf14",
          "url": "https://github.com/varpulis/varpulis/commit/20adff6948a4ef380cae09445b6f17d25a13a28d"
        },
        "date": 1772114050249,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33663,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331210,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3327600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40679,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 445590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2245800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32542,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 391700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1961800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2119300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3918600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1500000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1773000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 119660000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1750300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3416700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16880000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33587000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32938000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19962000,
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
          "id": "62ea38f3b08293c0baaefa6fe2a37f443dc8d2ea",
          "message": "fix(cdc): suppress dead_code warnings for binary pgoutput functions\n\nThe parse_pgoutput_message and parse_tuple_data functions implement\nthe binary pgoutput protocol path, which is tested but not yet wired\ninto the polling-based CDC source. Allow dead_code to fix CI clippy\nwith -D warnings.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T15:01:49+01:00",
          "tree_id": "fb39a7763c3575cfc9b4493ead61bae998f7c426",
          "url": "https://github.com/varpulis/varpulis/commit/62ea38f3b08293c0baaefa6fe2a37f443dc8d2ea"
        },
        "date": 1772114873811,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 365950,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3713000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48176,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 514960,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2583400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 35713,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 420600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2142400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2487400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 5009100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1688300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2000400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 146800000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1985900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3968000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 21144000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 41483000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 40045000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 23832000,
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
          "id": "a65b5c38a27e517dd660b6777ea774e3085a035c",
          "message": "chore: add prebuilt Dockerfile and quick-deploy script\n\nDockerfile.prebuilt uses a locally compiled binary for ~10s image builds\n(vs ~15 min for full compile). quick-deploy.sh chains local build → GHCR\npush → Hetzner pull for ~90s code-to-production cycles.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T19:38:40+01:00",
          "tree_id": "0f94c9aa55a13d9b6cade518d542ca94a7955155",
          "url": "https://github.com/varpulis/varpulis/commit/a65b5c38a27e517dd660b6777ea774e3085a035c"
        },
        "date": 1772131578697,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 36759,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 363230,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4178099,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48380,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 513640,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2751800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 36121,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 427140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2266100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2589000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 5223400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1748500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2048000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 148740000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2056600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4095100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 21689000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 42216000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 42632000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 24638000,
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
          "id": "74f51cf5f4a9f44ddfdf269a449e6f94a43c3a6f",
          "message": "fix(cluster): replicate heartbeat metrics through Raft for correct API responses\n\nWorkers send heartbeat metrics (events_processed, pipelines_running) to\ntheir assigned coordinator, but the /api/v1/cluster/workers endpoint\nforwards requests to the Raft leader. The leader had no heartbeat data,\nso metrics always showed 0.\n\nFix: replicate WorkerMetricsUpdated through Raft on each heartbeat.\nWhen the receiving coordinator is the Raft leader, it writes directly.\nWhen it's a follower, it forwards to the leader's /raft/write endpoint.\n\nAlso fixes sync_from_raft() which previously overwrote live heartbeat\nmetrics with stale Raft state (always 0). Now uses max(local, raft) for\nevents_processed and Raft value for pipelines_running.\n\nAdds regression tests for heartbeat metrics flow across:\n- Raft state machine (WorkerMetricsUpdated command)\n- Coordinator heartbeat processing\n- Worker metrics collection from TenantManager\n- Pipeline metrics collection from Engine\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T22:54:59+01:00",
          "tree_id": "6d286d9421b2d5db14e402933f0899a2e2c3c880",
          "url": "https://github.com/varpulis/varpulis/commit/74f51cf5f4a9f44ddfdf269a449e6f94a43c3a6f"
        },
        "date": 1772143266164,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34279,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 337170,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3417000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41468,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 453130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2238600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32755,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 391750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1977800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2140900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3881200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1483200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1722000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 122460000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1782900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3316300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16758000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33162999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33506999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20341000,
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
          "id": "240cd469e8005429a5dd2a8765a8baa29ec83360",
          "message": "fix(cluster): replicate per-pipeline metrics and connector health through Raft\n\nThe /api/v1/cluster/metrics endpoint (used by the monitoring view) was\nreturning empty pipelines because it forwards to the Raft leader, which\nhad no per-pipeline heartbeat data (only aggregate events_processed).\n\nExtend WorkerMetricsUpdated Raft command to include pipeline_metrics\n(events_in, events_out, connector_health per pipeline). The leader's\nsync_from_raft now populates worker_metrics from Raft state, so the\nmonitoring view sees real-time pipeline throughput and connector status\nregardless of which coordinator serves the request.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T23:16:38+01:00",
          "tree_id": "1dd05664bc2b8d063f5734fc6132904094458428",
          "url": "https://github.com/varpulis/varpulis/commit/240cd469e8005429a5dd2a8765a8baa29ec83360"
        },
        "date": 1772144625960,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33724,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 337970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3400400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40702,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 428810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2213500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33369,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 398150,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2005500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2106000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3922000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1467900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1699500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 119400000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1760600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3248700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16231000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32488000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33177999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19460000,
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
          "id": "779b4c98213f719d8587afd6c2e2f416611dbee5",
          "message": "fix(web-ui): fix monitoring throughput chart and WebSocket auth\n\n- Feed aggregate pipeline metrics into the metrics store from REST polling\n  so the ThroughputChart component actually receives data points\n- Pass API key as query param on WebSocket connections (browser WS API\n  doesn't support custom headers)\n- Respect coordinator URL setting for WebSocket base URL\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T01:20:41+01:00",
          "tree_id": "253f4856eff7282d0a872e1df2fcc5b48b1185aa",
          "url": "https://github.com/varpulis/varpulis/commit/779b4c98213f719d8587afd6c2e2f416611dbee5"
        },
        "date": 1772152005161,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33368,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3325600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39699,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 435940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2177500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32612,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1952100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2111800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3850400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1465200,
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
            "value": 118210000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1736500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3424800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17059000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34059000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33036000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 18993000,
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
          "id": "2a6ffed0d40296f72bd7b5b46db2115ba6faacc8",
          "message": "fix(web-ui): update minimatch to fix high severity ReDoS vulnerability\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T01:22:06+01:00",
          "tree_id": "73dac2e62bb8624dbd31209badc71e3f8aa3bb0f",
          "url": "https://github.com/varpulis/varpulis/commit/2a6ffed0d40296f72bd7b5b46db2115ba6faacc8"
        },
        "date": 1772152101138,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 343510,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3440400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40609,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 460840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2236900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 34450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 406810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2073900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2186800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4078000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1528900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1784400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 113530000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1826600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3459800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17493000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 35223000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 34692000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20620000,
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
          "id": "954f6d6b3190ca7db3e8b491668c0ebf068d1815",
          "message": "fix(cluster): harden security across varpulis-cluster crate\n\nAddress findings from security audit:\n- Fix path traversal in model upload (validate name against identifier rules)\n- Enforce HS256 algorithm and expiry in JWT validation\n- Wrap JWT secret in SecretString for zeroization on drop\n- Add RBAC (Viewer) to previously unauthenticated Prometheus endpoint\n- Escape quotes/backslashes in VPL connector parameter interpolation\n- Use constant-time comparison for Raft inter-node auth keys\n- Wrap WorkerNode.api_key and LlmConfig.api_key in SecretString\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T01:39:31+01:00",
          "tree_id": "e428f9e427ddeb9db5deab30f21b0365791e561f",
          "url": "https://github.com/varpulis/varpulis/commit/954f6d6b3190ca7db3e8b491668c0ebf068d1815"
        },
        "date": 1772153138258,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33362,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 330500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3343600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41181,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 454310,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2208500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32631,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 388430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1951600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2106900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4155100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1456500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1664600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 123910000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1808600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3385000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16843000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33717000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32857999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19211000,
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
          "id": "2eaa72899fd3c3a7bbcf63ab337dab17749afb21",
          "message": "fix(cdc): cast f64 params to numeric in CDC e2e tests\n\ntokio-postgres rejects binding f64 to NUMERIC(10,2) columns with\nWrongType error. Add explicit $N::numeric casts in all INSERT/UPDATE\nqueries so PostgreSQL accepts DOUBLE PRECISION input and casts to\nNUMERIC.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T01:43:02+01:00",
          "tree_id": "04d062875bf82dbc17b81ca74eac5498cedc8f7f",
          "url": "https://github.com/varpulis/varpulis/commit/2eaa72899fd3c3a7bbcf63ab337dab17749afb21"
        },
        "date": 1772153345009,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33701,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 342210,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3414100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41933,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 449620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2273700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33324,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 396260,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2037700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2141200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4040199,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1479600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1697400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 122160000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1741600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3306500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16530999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33072000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33968000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19919000,
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
          "id": "bd140cb08e7e6e3782b04f1a77d98a00d7e45c5e",
          "message": "fix(cli): wrap LLM API key in SecretString to match chat::LlmConfig\n\nThe security hardening commit changed LlmConfig.api_key from\nOption<String> to Option<SecretString>, but missed the construction\nsite in the CLI crate.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T01:47:37+01:00",
          "tree_id": "f120dacfcf2af6dfa0f91b474fea09a0072ef8be",
          "url": "https://github.com/varpulis/varpulis/commit/bd140cb08e7e6e3782b04f1a77d98a00d7e45c5e"
        },
        "date": 1772153616474,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33811,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 330970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3316900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39768,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 442820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2208300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32564,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 387090,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1954100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2133300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3986300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1457600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1665500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 119500000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1750800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3318500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16689000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33562000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33253999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19178000,
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
          "id": "6227d84c01f8cb9fdf277e3bb7a8ec5d31bc4836",
          "message": "fix(ci): fix CDC e2e type mismatch and NATS test SecretString errors\n\n- Change CDC test tables from NUMERIC(10,2) to DOUBLE PRECISION so\n  tokio-postgres can bind f64 values directly (fixes WrongType error)\n- Revert ineffective $N::numeric SQL casts in cdc_e2e.rs\n- Wrap WorkerNode.api_key in SecretString in nats_multi_worker_e2e.rs\n- Update docker-compose.cdc.yml schema to match\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T02:01:43+01:00",
          "tree_id": "bac8ab5d6cc7577494c5d6a05bdb6de7a6e3e48a",
          "url": "https://github.com/varpulis/varpulis/commit/6227d84c01f8cb9fdf277e3bb7a8ec5d31bc4836"
        },
        "date": 1772154460961,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33610,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3343500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39829,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 437400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2176100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32302,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1963200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2107700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3905900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1470800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1669700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 122160000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1732500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3283000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16277999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32780000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32564000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19097000,
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
          "id": "40ce710c7711f604414fe34b945010b06e2dabf9",
          "message": "fix(cdc): use test_decoding plugin for CDC replication slot\n\nThe CDC connector was creating slots with pgoutput (binary) but parsing\nwith parse_change_text() which expects test_decoding text format. Switch\nto test_decoding and drop pgoutput-specific query options.\n\nAlso fix cargo fmt on CLI main.rs.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T02:15:00+01:00",
          "tree_id": "e529c6ce567261a506b491636b3830747e8ccba0",
          "url": "https://github.com/varpulis/varpulis/commit/40ce710c7711f604414fe34b945010b06e2dabf9"
        },
        "date": 1772155249865,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33416,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331670,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3314700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40544,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 448740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2240900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33090,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 389860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2025500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2158200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3865100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1464800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1703400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 121660000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1719000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3281500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16697000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33052000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33198000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19073000,
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
          "id": "fd437f03a1624a5bee9b8fed941eaeb4e0fd0288",
          "message": "fix(ci): run CDC e2e tests serially to prevent cross-test contamination\n\ntest_decoding captures all WAL changes (no publication filtering),\nso concurrent tests sharing the same tables see each other's DML.\nRun with --test-threads=1 to isolate each test's replication slot.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T02:26:53+01:00",
          "tree_id": "6cad8a47690fa5b4626d280ee943491b1a906efe",
          "url": "https://github.com/varpulis/varpulis/commit/fd437f03a1624a5bee9b8fed941eaeb4e0fd0288"
        },
        "date": 1772156017508,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33807,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 325930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3240400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 38925,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 433560,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2166300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 383120,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1926800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2046400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3806200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1418800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1648400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115120000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1720700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3241900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 15827000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 31834000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32891000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 18853000,
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
          "id": "3673ce4e04f131d45804fa498e6f152294ce4e1f",
          "message": "fix(cdc): handle multi-word PostgreSQL types in test_decoding parser\n\nThe test_decoding WAL output uses type names like \"double precision\"\nwhich contain spaces. The previous split_whitespace() tokenizer broke\nthese into separate tokens, causing parse_change_text() to return None\nfor every row — resulting in 0 events in all CDC E2E tests.\n\nReplace split_whitespace() with a bracket-aware scanner that finds\nfield boundaries by scanning for '[' and ']' delimiters. Also drain\nstale WAL entries after slot creation to avoid cross-test contamination.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T02:50:39+01:00",
          "tree_id": "d3f665f35595019bb568a945fa1785036aec3cba",
          "url": "https://github.com/varpulis/varpulis/commit/3673ce4e04f131d45804fa498e6f152294ce4e1f"
        },
        "date": 1772157403333,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33216,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3285000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41012,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 453940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2232300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32805,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 393630,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1958100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2167200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3938400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1489800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1703900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 121900000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1750200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3317100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16696000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33430000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32909000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19478000,
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
          "id": "63d514f06eb1b1448db2c69996d9ee118affb23e",
          "message": "fix(docker): bust GHA layer cache with commit SHA to prevent stale builds\n\nThe Docker CI was serving stale cargo build layers from the GHA cache,\neven when source files changed. This caused the deployed binary to be\nmissing the WorkerMetricsUpdated Raft command and the /raft/write\nendpoint, breaking heartbeat metric replication across coordinators.\n\nReplace the unreliable `touch` cache invalidation with a COMMIT_SHA\nbuild arg that changes on every CI push, forcing a fresh cargo build.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T12:11:55+01:00",
          "tree_id": "ed5905238321997197629d28a359b418928f1a64",
          "url": "https://github.com/varpulis/varpulis/commit/63d514f06eb1b1448db2c69996d9ee118affb23e"
        },
        "date": 1772191139581,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33993,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 337360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3359200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40283,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 444290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2229300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32832,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 397260,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1976400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2191300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3912300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1474800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1738400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 118410000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1748900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3338900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16735000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33369000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33101999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19797000,
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
          "id": "3532454db0a8b195edc89251f95ecf102fcdb605",
          "message": "fix(web-ui): fix monitoring view throughput chart and WebSocket resilience\n\n- Separate fetchClusterMetrics from fetchWorkers/fetchGroups so a failure\n  in non-critical requests doesn't block the throughput chart update\n- Use aggregate throughput calculation (matching MetricsView approach)\n  instead of per-pipeline sum for the chart data\n- Handle stale WebSocket connections: reconnect when readyState is CLOSED\n- Set maxRetries to Infinity so the live event stream recovers from\n  transient coordinator outages\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T12:30:02+01:00",
          "tree_id": "e1279d485eb56af9d043d5a8924709fc02f18ee7",
          "url": "https://github.com/varpulis/varpulis/commit/3532454db0a8b195edc89251f95ecf102fcdb605"
        },
        "date": 1772192166745,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33428,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3268200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40403,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 445910,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2192500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32729,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1948900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2121700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3922100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1471600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1664300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 122430000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1771800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3264400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16428000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32774000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32450000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19462000,
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
          "id": "96295041ab2550edf43d1d7c8f77ade661dfcc5e",
          "message": "fix(engine): forward .to() sink events to WebSocket output channel\n\nEvents sent to connector sinks via .to() were only counted but not\nforwarded to the output channel, making them invisible in the live\nevent stream. Now .to() sink output events are forwarded through the\nWebSocket relay alongside .emit() and .process() events.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T13:02:32+01:00",
          "tree_id": "ae2c2f2b75ed7849bb3cec375f7c4933194102f9",
          "url": "https://github.com/varpulis/varpulis/commit/96295041ab2550edf43d1d7c8f77ade661dfcc5e"
        },
        "date": 1772194176732,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33255,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 329710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3360000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39579,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 439710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2175900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33591,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 381480,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1951800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2187800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3953400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1495700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1714400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 122770000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1769200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3256500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16169000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32366999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32813000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19140000,
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
          "id": "3d33fd4fba85d2ebf9c41a19c93d8b5d7c6dda69",
          "message": "fix(parser): lower nesting depth limit to prevent fuzz timeout (DoS)\n\nThe scheduled fuzzer found inputs with ~20 levels of unclosed brackets\nmixed with `if` keywords that cause O(k^depth) exponential backtracking\nin pest's PEG parser (>1200s on 94 bytes).\n\n- Lower MAX_NESTING_DEPTH from 24 to 16 (typical VPL is 3-6 levels)\n- Add -timeout=30 to fuzz workflow as defense-in-depth\n- Add regression test with exact fuzzer crash/slow-unit inputs\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T13:08:09+01:00",
          "tree_id": "f89c6551c63cb4b80f674426711170ad1e60ec0e",
          "url": "https://github.com/varpulis/varpulis/commit/3d33fd4fba85d2ebf9c41a19c93d8b5d7c6dda69"
        },
        "date": 1772194528982,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33515,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 336250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3367900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39796,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 446820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2190200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32433,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 386540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1949900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2107400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3892400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1467000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1753200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 118460000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1726300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3335000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16562999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33287000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32982999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19099000,
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
          "id": "4e5fbc00d379a6532172762f34d4c149cfd47fcb",
          "message": "fix(parser): lower nesting depth to 10 after second fuzz timeout at depth 16\n\nThe first fix (depth 24→16) was bypassed by a fuzzer input with exactly\n16 unclosed brackets (39s timeout). Measured backtracking scales as\nO(2.35^depth): depth 20→1200s, depth 16→39s, depth 10→<0.3s.\n\n10 levels is still generous for real VPL (typical 3-6, extreme ~8).\nAdds regression tests for all three fuzzer crash inputs.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T13:49:06+01:00",
          "tree_id": "8c325f3a3ad890f8bde8014d7b30693368976d3a",
          "url": "https://github.com/varpulis/varpulis/commit/4e5fbc00d379a6532172762f34d4c149cfd47fcb"
        },
        "date": 1772196970815,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 37128,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 366790,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3802300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 48068,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 508540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2563700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 35976,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 419590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2172200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2483600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4996100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1688800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2046100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 148400000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1997600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3889800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 20618000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 41464000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 40929000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 24253000,
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
          "id": "3da0832bb9d20c3df57e89c4749497e0ffb40b62",
          "message": "fix(web-ui): remove misleading dev-key references and non-functional API Keys card\n\nThe demo uses VITE_API_KEY at build time so visitors never need to enter\na key. Remove the \"Use Dev Key\" button, \"dev-key\" hints, and the API Keys\nmanagement card (generate/revoke) which calls org-based endpoints that\ndon't exist in the demo deployment.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-27T18:27:13+01:00",
          "tree_id": "3a054bffdc36fe8df9d60fb71d092bd1e35253d1",
          "url": "https://github.com/varpulis/varpulis/commit/3da0832bb9d20c3df57e89c4749497e0ffb40b62"
        },
        "date": 1772213608544,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 37018,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 365150,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3652300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 42640,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 468920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2272700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 34369,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 423590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2140500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2341600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4281800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1481300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1821300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 117320000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1866000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3687500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 18540000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 37118000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 35826000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20307000,
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
          "id": "36f57e1e93e7a46b343b757760f2eb98446ed67b",
          "message": "chore(release): bump version to 0.4.1\n\n- Bump workspace version to 0.4.1\n- Update CHANGELOG with 0.4.1 release section\n- Update Homebrew formula and install script to 0.4.1\n- Add homebrew tap auto-update job to release workflow\n- Create varpulis/homebrew-tap repo for `brew install varpulis/tap/varpulis`\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T00:35:01+01:00",
          "tree_id": "f3a84f4b687885f690f31e7874ef563d88e14b87",
          "url": "https://github.com/varpulis/varpulis/commit/36f57e1e93e7a46b343b757760f2eb98446ed67b"
        },
        "date": 1772235718428,
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
            "value": 330310,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3277900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39959,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 443250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2215100,
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
            "value": 386590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1970500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2130400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3914500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1476600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1674700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 111730000,
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
            "value": 3298200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16504999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33034999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32993000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19022000,
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
          "id": "89e0f6f1672753d8dea6c48a4460e9b57ecd7cf1",
          "message": "fix(release): add version fields for crates.io and repository metadata\n\n- Add version = \"0.4.1\" to all inter-crate path dependencies (required by crates.io)\n- Add repository.workspace = true to all publishable crates\n- Use HOMEBREW_TAP_TOKEN for cross-repo push in release workflow\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T01:28:06+01:00",
          "tree_id": "ade76764f98f01e639101a735a8a1830b7902e09",
          "url": "https://github.com/varpulis/varpulis/commit/89e0f6f1672753d8dea6c48a4460e9b57ecd7cf1"
        },
        "date": 1772238856476,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33909,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 342310,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3389000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41435,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 446710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2239100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32323,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 394720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1958100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2201000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3940000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1460000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1669700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 111480000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1746500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3296000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16512000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33638000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 34778000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 20343000,
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
          "id": "73ff2d5f01d69b33e81fad6bf19469f52709f49b",
          "message": "fix: resolve audit issues #47-#57\n\n- #47: Rename worker_pool::BackpressureError → PoolBackpressureError\n- #48: Add Content-Security-Policy header to nginx config\n- #49: WebSocket auth via cookie instead of URL query param\n- #50: Consolidate AuthError types in varpulis-cli\n- #51: Replace URI parsing unwraps with proper error handling\n- #52: Reduce duplicate dependency versions via workspace unification\n- #53: CORS defaults to restrictive same-origin when --cors-origins unset\n- #54: Warn on weak JWT_SECRET in production mode\n- #55: Fix 18 ignored doc-tests with proper no_run annotations\n- #56: Decompose 6298-line sase.rs into 18 focused submodules\n- #57: Add Permissions-Policy and commented HSTS header to nginx\n\nCloses #47, #48, #49, #50, #51, #52, #53, #54, #55, #56, #57\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T15:31:55+01:00",
          "tree_id": "10e27f1d50fe847803910368673b5a303bf18a14",
          "url": "https://github.com/varpulis/varpulis/commit/73ff2d5f01d69b33e81fad6bf19469f52709f49b"
        },
        "date": 1772289571648,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 32884,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 325640,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3324000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39784,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 444440,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2193900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32028,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 383850,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1931500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2100700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3881200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1452900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1671600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 114530000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1733600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3225400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16142000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32448999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32603000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19341000,
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
          "id": "8005bc897137ed6928f06cb59d7f1262c8685230",
          "message": "fix(persistence): replace deprecated GenericArray::from_slice with array conversion\n\nThe `from_slice` method on GenericArray is deprecated in generic-array 1.x.\nUse `TryInto<[u8; 12]>` + `Nonce::from()` instead.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T15:55:16+01:00",
          "tree_id": "3ab9be3647d1456628c97717761b6e729cff7513",
          "url": "https://github.com/varpulis/varpulis/commit/8005bc897137ed6928f06cb59d7f1262c8685230"
        },
        "date": 1772290877523,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33021,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 328600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3297300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40034,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 442350,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2182500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32081,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 385850,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1947900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2119500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3925000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1474200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1681300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 115360000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1755700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3250500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16231000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32548000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32668000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19276000,
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
          "id": "18bad5741e276e6c7000de24382d19379b776845",
          "message": "refactor: move sase and engine tests from src/ to tests/\n\nUnit tests in src/ can only be justified when they test private internals.\nBoth sase/tests.rs and engine/tests.rs only used the public API, so move\nthem to the integration test directory where they belong.\n\n- sase/tests.rs → tests/sase_tests.rs (89 tests)\n- engine/tests.rs → tests/engine_tests.rs (114 tests)\n- classify_predicate and PredicateClass promoted to pub (were pub(crate))\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T16:23:52+01:00",
          "tree_id": "afde8e24f6b9af29642ae2bcef37de1cb37ce10b",
          "url": "https://github.com/varpulis/varpulis/commit/18bad5741e276e6c7000de24382d19379b776845"
        },
        "date": 1772292664827,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 34430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 339720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3343500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40776,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 460790,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2265900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32404,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 387690,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1956200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2254400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4133700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1455800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1711100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 117840000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1777100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3314300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16690000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33171999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33879000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19733000,
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
          "id": "5186cbacda846dff02206c7ef39349930b87316d",
          "message": "feat: Phase 1 architecture improvements (#58, #60, #64)\n\nActor framework (varpulis-actors crate):\n- Actor, Handler, Mailbox, Supervisor, Runtime traits and types\n- Typed message passing with bounded channels and request-reply\n- Supervision trees with restart policies (Always/OnFailure/Never)\n- Observable state for health monitoring\n- 8 unit tests + 2 integration tests + doctest\n\nWarp → Axum migration:\n- Migrate varpulis-cli (9 files) and varpulis-cluster (4 files) from warp 0.3 to axum 0.8\n- Replace filter chains with Router + handler functions\n- Auth middleware via axum::middleware::from_fn_with_state\n- WebSocket via axum::extract::ws::WebSocketUpgrade\n- Rate limiting as tower middleware\n- All tests converted to tower::ServiceExt::oneshot\n- 232 CLI + 201 cluster tests pass\n\nDocumentation and UI:\n- ADR-006 (actor framework), ADR-007 (axum migration)\n- design/decisions/ directory with index and template\n- Health types (TypeScript) and Pinia store for actor health\n- WebSocket actor_health message handler\n- README architecture section, CONTRIBUTING ADR process\n\nCloses #58, #60, #64\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T19:08:13+01:00",
          "tree_id": "76782cfe9d2c9e84a2d2f4de93d4bf014a230eda",
          "url": "https://github.com/varpulis/varpulis/commit/5186cbacda846dff02206c7ef39349930b87316d"
        },
        "date": 1772302454806,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 32990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 326650,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3323800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39581,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 436280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2199500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 31963,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 381330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1943900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2128600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3863900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1466100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1667200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 117710000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1752900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3305100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16600000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33017000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33832000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19000000,
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
          "id": "12589781ab9e80347208111df0bf6a25aa5f4f91",
          "message": "fix: resolve CI clippy and TypeScript lint failures\n\n- Remove unused ActorHealthStatus import in web-ui health store\n- Allow dead_code on NumberEvent(u64) in actor integration test\n- Remove unnecessary borrows on format!() in cluster integration tests\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T19:29:41+01:00",
          "tree_id": "341da33e25473eab74e7fff4ace3d08933154d0f",
          "url": "https://github.com/varpulis/varpulis/commit/12589781ab9e80347208111df0bf6a25aa5f4f91"
        },
        "date": 1772303736253,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33975,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 342370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3441200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40076,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 446530,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2267300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32232,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 392420,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2054600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2134800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3879500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1479000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1673800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 117090000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1747100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3289000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16175999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 33054000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32911000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19125000,
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
          "id": "98774d1976b286a67a4e64d997b9452d97d1b6b7",
          "message": "feat: Phase 2 architecture improvements (#65, #66, #68)\n\nImplement three interconnected architectural improvements:\n\n- Declarative component registration using inventory crate (#65):\n  ConnectorFactory trait with inventory::submit! replaces three\n  separate match-arm dispatch tables for connector creation.\n  All 12 connectors registered; dispatch falls back to match arms\n  during migration.\n\n- Logical/physical plan separation with optimizer (#68):\n  Three-stage compilation pipeline (AST → LogicalPlan → Optimizer →\n  PhysicalPlan). Serializable plan types enable EXPLAIN, four\n  optimization rules (filter pushdown, temporal filter pushdown,\n  window merge, projection pruning).\n\n- DAG topology with builder pattern (#66):\n  Explicit inter-stream topology with TopologyBuilder, Kahn's\n  algorithm for topological ordering, cycle detection, and JSON\n  serialization for Vue Flow visualization.\n\nIncludes UI components (TopologyView, ExplainView, ConnectorsView\navailable types tab), API/store updates, and ADRs 008-010.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T23:20:05+01:00",
          "tree_id": "198c3ed16d21d227a330230c49c031288f6f942e",
          "url": "https://github.com/varpulis/varpulis/commit/98774d1976b286a67a4e64d997b9452d97d1b6b7"
        },
        "date": 1772317635198,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33699,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3340800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40826,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 446540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2230700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32864,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 396690,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1983800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2127400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3909700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1472000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1680700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 109220000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1757300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3447500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17391000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 34768000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33198000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19398000,
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
          "id": "a781b80b8c2da39d32ffbc74e42fa264ec6ec5dc",
          "message": "fix: wire PhysicalPlan into Engine::load_program to eliminate dead-code warnings\n\nPhysicalPlan is now populated during load_program() with metadata about\neach materialized stream (name, operation count/summary, event types).\nThis fixes CI clippy -D warnings for unused code and provides a\nphysical_plan_summary() accessor for debugging.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T23:36:31+01:00",
          "tree_id": "c5ebf20d8e3002b65d1111984f4f7afdff94d236",
          "url": "https://github.com/varpulis/varpulis/commit/a781b80b8c2da39d32ffbc74e42fa264ec6ec5dc"
        },
        "date": 1772318568873,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 32940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 327140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3356700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 41159,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 450310,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2252700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32058,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 388040,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1960800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2111600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4162899,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1504400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1705300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 116020000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1778600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3332400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16378000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32725000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32759999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19551000,
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
          "id": "2912845ca66f035ab7eda99e820e9bfe2710a958",
          "message": "fix: unused variable in redis factory when feature is enabled\n\nMove `channel` binding inside the cfg(not(feature)) block where it's\nactually used, preventing dead-code warning under -D warnings.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-28T23:51:47+01:00",
          "tree_id": "4c331b89109fdc91323136afded38b7a2de48528",
          "url": "https://github.com/varpulis/varpulis/commit/2912845ca66f035ab7eda99e820e9bfe2710a958"
        },
        "date": 1772319541057,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33277,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 331380,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3281800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 39905,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 443560,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2209700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 386850,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1949500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2104000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3907700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1468200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1656100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 108750000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1809300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3243900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16178000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32320999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32732000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19035000,
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
          "id": "95613114da4234cc8a487722caa6b0b483b99c01",
          "message": "docs(readme): restructure performance section with per-layer benchmarks\n\nThe previous \"250K+ evt/s\" headline was from CLI simulation mode, which\nundersells the core engine. Add Criterion benchmark results at each layer:\n\n- Core SASE+ engine: 1.5M evt/s (sequence), 1.1M evt/s (Kleene)\n- Full VPL pipeline: 410K evt/s (filter+emit), 1.4M evt/s (aggregate)\n- CLI end-to-end: 234-268K evt/s (with JSONL parsing + stdout)\n- MQTT I/O-bound: 6K evt/s (broker ceiling)\n\nEach section now clearly states what is being measured.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-03-01T00:55:32+01:00",
          "tree_id": "25d9a176e54defc05b2da978c393bc47e2ec3375",
          "url": "https://github.com/varpulis/varpulis/commit/95613114da4234cc8a487722caa6b0b483b99c01"
        },
        "date": 1772323305154,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33166,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 328220,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3295000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40429,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 447790,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2214200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32347,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 398200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1965900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2092699,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3903800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1463500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1675800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 111860000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1767100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3291700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16323000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32582000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 33290999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19515000,
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
          "id": "f269cd2136c5bc90e8aa326db4321ef1f52279d9",
          "message": "feat: Phase 3 architecture improvements (#61, #62, #63, #67, #69, #72)\n\nAdd six cross-cutting infrastructure features to the runtime:\n\n- Health monitoring (#62): HealthStatus/HealthReporter/HealthRegistry with\n  REST probes (/health/live, /health/ready, /health/started, /health)\n- Converter trait (#61): standardized serialization boundary with\n  JsonConverter and feature-gated CsvConverter\n- Time-accelerated testing (#72): SimulatedScheduler and TestUniverse\n  for deterministic, instant VPL program testing\n- Fixture-based test discovery (#63): insta snapshot golden tests for\n  both runtime engine and parser with macro-generated test functions\n- UDFs with type signatures (#69): ScalarUDF/AggregateUDF traits,\n  Accumulator, UdfRegistry wired into engine evaluator and compiler\n- Backpressure strategies (#67): WhenFull/StageBuffer/StageBufferMetrics\n  for configurable per-stage buffer management\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-03-01T00:56:11+01:00",
          "tree_id": "a604fb91e47682cf33c572e2ab6ed5d14ad12c35",
          "url": "https://github.com/varpulis/varpulis/commit/f269cd2136c5bc90e8aa326db4321ef1f52279d9"
        },
        "date": 1772323361055,
        "tool": "cargo",
        "benches": [
          {
            "name": "simple_sequence/sase/100",
            "value": 33472,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 333800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3263800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 40111,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 438990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2160600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 32360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 388980,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 1972700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2130100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3951200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1499600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1670200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 110670000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1763100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3240800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 16353000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 32408000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 32668999,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 19140000,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1784531679357,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 64918,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2113900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 693920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2369500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 6889400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 5347900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1328000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2671800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 13384000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 9099100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 66941000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 45385000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4253100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 7107800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 20325000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1849800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2632100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 9359300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 5048000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 39722000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 17189000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2374500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2304500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2321000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 5382600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 4405300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 4410400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 501860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2214000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 2075300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2598200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 10752000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 5639400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 502490,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 535540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 2068299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 2199000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 10766000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 11407000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 678860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1426600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 30697,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 58645,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 271130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1742100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 661740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 10328000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 830750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 47900000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 1119200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 159200000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2550,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 4414,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 7817,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 15306,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 14709,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 29231,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 63611,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 138250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 124890,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 273320,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 833100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 12166000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 200640,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 563560,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 5018100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 553940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 43536000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 490780000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 1177700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1655200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 645300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1380100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 55123,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 268860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 548330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2657500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 3268100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 4912200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 18599000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 34918000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 42005,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 82029,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 131160,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 188460,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 54742,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 264790,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 528390,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2730200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 43595,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 156680,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 298690,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 158010,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 157770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 159850,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 156470,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 160280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 67464,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 95271,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 102090,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 45857,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 455710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4555800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 61621,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 614240,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 3090600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 43247,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 531180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2706800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2642300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4891900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 2009199,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2262900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 293940000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2317700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4389400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 22440000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 45371000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 45794000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 29354000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 128240,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 154830,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 1248500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1487200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 11741000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 14715000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 40,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 57,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 35,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 87,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 128,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 2235300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 16807000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 16871000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 7353400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 7316400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 7349700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 9161500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 197,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 3188700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 24852000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 179,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 6736500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 45420000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 398,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 8455600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 55473000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 156,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 8913600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 2357500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 2361600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 949650,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 4974500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 4278,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1785137349445,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 50242,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2097800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 537280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2270300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 5358300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 4015900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1042200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2530900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 10607000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 7665000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 52623000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 33973000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4213900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 6782800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 17645000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1415000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2490700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 7082000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 4282600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 28939000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 12102000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2275900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2254700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2250300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 4033500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 3725500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 3754200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 394410,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2166900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 1616400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2426400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 8351699,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 3728300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 395330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 388670,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 1613600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 1595100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 8284400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 8208100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 524840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1084800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 24552,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 45699,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 210540,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1359600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 499800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 7849500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 642280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 36330000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 867030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 124270000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2078,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 3393,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 5758,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 11793,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 10798,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 22419,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 49334,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 105730,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 97717,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 210610,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 652070,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 9823600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 156510,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 437060,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 4021899,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 425300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 34756000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 391180000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 954030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1277500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 492420,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1156800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 42549,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 212680,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 424030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2095600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 2512000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 3779200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 13958000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 26726000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 33381,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 63831,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 103800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 147230,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 42243,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 208310,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 415450,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2137200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 33679,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 121880,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 230030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 125610,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 124470,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 125520,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 124840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 124840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 52515,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 75926,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 80197,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 35729,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 353460,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 3530400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 50644,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 502240,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2515400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 33653,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 412040,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2107200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2080700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 3862700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1533300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 1831500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 246370000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 1847600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 3491600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 17816000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 35530000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 35930000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 22945000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 98503,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 119210,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 1043700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1228700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 9984000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 12178000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 31,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 52,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 27,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 59,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 67,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 101,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 1735200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 13401000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 13056000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 5915700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 5924600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 5923500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 7293600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 155,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 2448400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 15492000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 140,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 5101100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 29573000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 308,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 6118900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 29252000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 120,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 6371600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 1825900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 1831600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 736420,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 3851800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 3310,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1785742111516,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 62018,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2114100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 652290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2339800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 6568300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 4606000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1247400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2661800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 12620000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 8968700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 63222000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 39748000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4254700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 7014700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 18443000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1786700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2612400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 9199100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 4904800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 38699000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 14802000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2350300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2317000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2318500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 4636900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 4204200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 4211800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 483300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2210200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 1964900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2537600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 10411000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 4374600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 485070,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 482700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 1958800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 1970700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 10449000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 10517000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 728160,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1539800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 32811,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 64997,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 292960,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1805800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 679010,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 10412000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 855650,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 47057000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 1142500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 160440000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2801,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 4734,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 7988,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 16686,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 14921,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 31578,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 69592,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 151360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 139160,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 303580,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 874000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 12640000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 217740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 614600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 5106600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 616300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 42236000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 484790000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 1289000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1828600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 684420,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1504900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 52495,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 267810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 537330,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2600400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 3166400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 4674000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 18001000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 32649000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 40689,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 82032,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 131220,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 182720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 53459,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 259810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 524510,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2676200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 41854,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 148280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 281810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 163740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 162020,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 160910,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 159390,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 161000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 69327,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 98276,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 105710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 42953,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 422920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4291500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 61949,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 593250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2997100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 41223,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 502740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2521400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2562600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4643700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1912400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2189600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 428400000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2213100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4236600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 21218000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 42442000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 42417000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 28102000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 139830,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 171120,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 1296100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1632800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 12834000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 16353000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 42,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 63,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 36,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 79,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 82,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 127,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 2459000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 18920000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 19149000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 8028300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 8066500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 8058299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 10937000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 3558300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 20853000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 161,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 6628900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 34056000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 365,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 7684500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 37284000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 153,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 8122299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 2156000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 2099400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 872810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 4561600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 3782,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1786341208080,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 62454,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2122200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 665740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2334300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 6594300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 4541500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1267600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2675000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 12693000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 9154700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 63628000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 41165000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4261500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 7039000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 18671000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1785700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2623700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 9147900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 5162100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 39012000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 14875000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2350200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2323700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2324400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 4590900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 4169900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 4170700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 485390,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2216700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 2004100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2555800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 10647000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 4338100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 484280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 490640,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 2012600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 2023100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 10677000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 10764000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 668620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1364500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 30679,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 57207,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 276570,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1641300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 647080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 9578200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 825950,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 46164000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 1110300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 156990000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2575,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 4373,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 7357,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 15056,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 13893,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 28342,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 63868,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 134820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 127590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 271250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 829370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 11698000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 206750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 542360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 4651200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 540010,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 39760000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 466060000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 1219500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1617600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 626370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1338800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 51506,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 258290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 528290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2612300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 3147100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 4671800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 17573000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 33560000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 40976,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 78995,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 129120,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 178760,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 54733,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 264670,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 527040,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2619600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 41137,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 146550,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 278970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 159280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 157810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 158990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 158590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 157860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 66244,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 93165,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 100290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 42929,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 428110,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4290100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 60778,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 601570,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 2976800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 41283,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 504940,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2552200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2566200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4790500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1856300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2209600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 431320000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2202600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4298900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 21388000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 42289000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 44250000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 28529000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 134010,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 162810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 1277400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1788700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 12716000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 16204000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 44,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 63,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 36,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 82,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 81,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 127,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 2458900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 18905000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 18995000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 8233499,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 8217300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 8109700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 11276000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 184,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 3588400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 23340000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 161,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 6568100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 37104000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 368,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 7857200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 42509000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 159,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 8222899,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 2156300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 2102800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 874550,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 4539000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 3787,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1786943279371,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 63107,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2111100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 671300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2344700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 6773200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 4738000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1292600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2674500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 12870000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 9030900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 65614000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 43634000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4252300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 7070200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 19683000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1812800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2628400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 9103700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 5029700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 38701000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 15609000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2347800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2289700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2295000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 4760000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 4391700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 4389400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 502720,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2191700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 2057300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2528900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 10773000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 4699800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 498480,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 500840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 2044500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 2083600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 10437000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 10584000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 683370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1400900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 31775,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 58905,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 276770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1773700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 653130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 10021000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 832050,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 46388000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 1122200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 161590000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2634,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 4425,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 7756,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 15497,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 14282,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 29249,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 63911,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 137860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 127130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 274970,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 839210,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 12591000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 202230,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 568400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 4990400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 554300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 42650000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 504700000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 1236600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1659300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 636860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1376500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 52855,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 263250,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 534080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2744800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 3234700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 4907700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 18130000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 34769000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 41398,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 80628,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 132080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 184130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 54743,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 265690,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 531820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2677700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 44110,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 156240,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 297210,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 159620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 162750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 162790,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 160340,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 159340,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 67134,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 98033,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 102840,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 45868,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 454240,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4533100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 63767,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 628160,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 3125400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 43712,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 542800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2695900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2763500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 5049400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 2006300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2293100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 310400000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2328300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4413700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 22483000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 44306000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 46352000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 29189000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 127570,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 159100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 1222900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1488400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 12195000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 14635000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 41,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 57,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 35,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 76,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 87,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 128,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 2243600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 16395000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 16631000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 7397000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 7384500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 7531300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 9282400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 197,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 3125300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 19072000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 178,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 6447500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 34710000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 392,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 7926400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 37385000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 155,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 8268000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 2356800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 2371500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 951980,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 4971100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 4273,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1787548274304,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 66344,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2123500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 724670,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2454700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 7241000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 6801000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1346400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2848700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 13559000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 10907000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 69212000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 50161000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4252900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 7816500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 32481000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1827000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2750100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 9248600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 6212700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 38482000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 18264000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2455800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2411900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2416000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 6759800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 6088300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 6020600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 488080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2239000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 2013399,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2756400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 10648000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 7334000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 483990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 497700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 2026400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 2049700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 10423000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 10655000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 584770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1084900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 27761,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 46224,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 257459,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1601900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 595710,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 8845500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 771740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 42659000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 1050900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 141720000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2278,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 3640,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 6299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 11913,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 11897,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 22382,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 56386,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 105490,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 109650,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 209990,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 776200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 10730000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 176460,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 432290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 3005800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 428570,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 38600000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 414350000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 1320300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1264500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 523760,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1027700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 54275,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 290770,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 592800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2930000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 3468600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 4863400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 16609000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 31025000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 46948,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 97832,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 162730,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 235550,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 58719,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 290370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 580000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2949600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 47430,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 160500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 307360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 195280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 197560,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 197550,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 196610,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 198860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 84332,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 122830,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 129750,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 46895,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 466820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4649700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 70067,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 686360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 3490800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 43920,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 518919,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2662100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2855200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 5593500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 2057700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2386200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 238990000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2369800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4677800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 24456000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 50537000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 48950000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 32820000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 97712,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 118000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 952260,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1154100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 9265200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 11717000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 31,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 15,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 42,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 56,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 71,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 101,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 1754900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 14372000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 14206000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 5741700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 5824700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 5887800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 7796200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 137,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 2819200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 20179000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 124,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 6618300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 36851000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 306,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 8404200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 40820000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 122,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 8955500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 2076700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 2126100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 745810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 4977800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 3064,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cyril PODER",
            "username": "cpoder",
            "email": "cyril.poder@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53",
          "message": "docs(connectors): fix broken intra-doc link that failed the Documentation CI job (#214)\n\n`set_source_pause_handle`'s doc comment linked to its sibling method with a bare\nname — `[` set_engine_offsets_registry `]` — which rustdoc can't resolve (methods\nneed a type-qualified path), so `cargo doc` under `RUSTDOCFLAGS=-D warnings`\nfailed with \"unresolved link to `set_engine_offsets_registry`\" → \"could not\ndocument `varpulis-connectors`\" (the red Documentation job on the v0.11.0 tag CI).\n\nQualify it as `[` Self::set_engine_offsets_registry `]` to keep the working\ncross-reference.\n\nVerified: `RUSTDOCFLAGS=\"-D warnings\" cargo doc -p varpulis-connectors --no-deps`\nnow finishes clean (was: the unresolved-link error above).\n\n\nClaude-Session: https://claude.ai/code/session_0173vR5wHzbtcHxj5ZcdB4AP\n\nCo-authored-by: cpoder <cyril.poderà@gmail.com>\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>",
          "timestamp": "2026-07-17T15:01:15Z",
          "url": "https://github.com/varpulis/varpulis/commit/fac85200dbcf79ab4cc8f25d064d3e7ba91c4f53"
        },
        "date": 1788173607364,
        "tool": "cargo",
        "benches": [
          {
            "name": "single_stream_overhead/no_context/100",
            "value": 62397,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/100",
            "value": 2122500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/1000",
            "value": 653620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/1000",
            "value": 2342100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/no_context/10000",
            "value": 6485900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_stream_overhead/with_context/10000",
            "value": 4584700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/1000",
            "value": 1266400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/1000",
            "value": 2666200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/10000",
            "value": 12671000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/10000",
            "value": 8691600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/sequential/50000",
            "value": 63156000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_streams/parallel/50000",
            "value": 40560000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/100",
            "value": 4261300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/1000",
            "value": 7000700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cross_context_pipeline/3_context_chain/10000",
            "value": 18531000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/1000",
            "value": 1825400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/1000",
            "value": 2600800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/5000",
            "value": 9142500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/5000",
            "value": 4775800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/sequential/20000",
            "value": 39190000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "cpu_intensive_parallel/parallel/20000",
            "value": 14128000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/1000",
            "value": 2348400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/1000",
            "value": 2314600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/1000",
            "value": 2325700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/process_async/10000",
            "value": 4596800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/try_process/10000",
            "value": 4214900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dispatch_methods/router_dispatch/10000",
            "value": 4186200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/500",
            "value": 485780,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/500",
            "value": 2205300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/2000",
            "value": 2000200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/2000",
            "value": 2526200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/no_context/10000",
            "value": 10497000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window/with_context/10000",
            "value": 4344000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/500",
            "value": 487350,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/500",
            "value": 490080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/2000",
            "value": 1997200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/2000",
            "value": 1998400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/event_driven/10000",
            "value": 10375000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "session_window_sweep/with_sweep/10000",
            "value": 10320000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/hamlet",
            "value": 664910,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "single_query/zdd_unified",
            "value": 1362200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/1",
            "value": 30361,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/1",
            "value": 57468,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/5",
            "value": 274800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/5",
            "value": 1606200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/10",
            "value": 638240,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/10",
            "value": 9414800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/25",
            "value": 823030,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/25",
            "value": 45875000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/hamlet/50",
            "value": 1102900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_query_scaling/zdd_unified/50",
            "value": 155620000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/10",
            "value": 2561,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/10",
            "value": 4312,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/50",
            "value": 7393,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/50",
            "value": 14974,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/100",
            "value": 13902,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/100",
            "value": 28299,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/500",
            "value": 62646,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/500",
            "value": 134950,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/hamlet/1000",
            "value": 127010,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_length/zdd_unified/1000",
            "value": 270820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/hamlet_shared",
            "value": 826090,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "shared_kleene/zdd_unified_shared",
            "value": 11658000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_bursty",
            "value": 207590,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_bursty",
            "value": 547290,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/hamlet_regular",
            "value": 4501600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "burstiness/zdd_regular",
            "value": 541930,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/hamlet_10k",
            "value": 39393000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/zdd_10k",
            "value": 466780000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_many_small",
            "value": 1222500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_many_small",
            "value": 1688000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/hamlet_few_large",
            "value": 620860,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_pressure/zdd_few_large",
            "value": 1393200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/100",
            "value": 56204,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/500",
            "value": 285740,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/1000",
            "value": 571620,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_simple/seq_a_b_plus_c/5000",
            "value": 2802500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/500",
            "value": 3309400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/1000",
            "value": 4902700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/5000",
            "value": 17617000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_rising_sequence/rising_prices/10000",
            "value": 34497000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/5middles_2^5_combos/31",
            "value": 45219,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/10middles_2^10_combos/1023",
            "value": 91212,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/15middles_2^15_combos/32767",
            "value": 148600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_exponential/20middles_2^20_combos/1048575",
            "value": 209050,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/100",
            "value": 56908,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/500",
            "value": 282080,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/1000",
            "value": 559440,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_star/seq_a_b_star_c/5000",
            "value": 2837800,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/100",
            "value": 45195,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/500",
            "value": 159690,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "all_pattern/all_matching/1000",
            "value": 302820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/10",
            "value": 183350,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/50",
            "value": 183280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/100",
            "value": 179810,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/500",
            "value": 181360,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "max_runs_impact/max_runs/1000",
            "value": 182160,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^15_combinations",
            "value": 76343,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^20_combinations",
            "value": 109310,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "memory_efficiency/2^25_combinations",
            "value": 115050,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/100",
            "value": 42803,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/1000",
            "value": 424130,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "simple_sequence/sase/10000",
            "value": 4284900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/100",
            "value": 60137,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/1000",
            "value": 608820,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "kleene_plus/sase/5000",
            "value": 3017600,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/100",
            "value": 41616,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/1000",
            "value": 499910,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "with_predicates/sase/5000",
            "value": 2529900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_5_events_5k",
            "value": 2631700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "long_sequence/seq_10_events_10k",
            "value": 4765000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/negation_5k",
            "value": 1942400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/or_pattern_5k",
            "value": 2188200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "complex_patterns/nested_kleene_5k",
            "value": 431280000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "multi_predicates/chained_predicates_5k",
            "value": 2176900,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/10000",
            "value": 4262500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/50000",
            "value": 21179000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "throughput/seq_3/100000",
            "value": 42854000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/100k_simple_seq",
            "value": 42251000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "scalability/50k_kleene_plus",
            "value": 30183000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/1000",
            "value": 133440,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/1000",
            "value": 162280,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/10000",
            "value": 1264300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/10000",
            "value": 1577300,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/cyclic_10_types/100000",
            "value": 12590000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_training/variable_pattern/100000",
            "value": 18048000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/1",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/1",
            "value": 44,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/3",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/3",
            "value": 61,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/5",
            "value": 36,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/5",
            "value": 80,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_symbol/10",
            "value": 81,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pst_prediction/predict_distribution/10",
            "value": 129,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_no_runs",
            "value": 2434200,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_with_runs",
            "value": 18817000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "pmc_forecast/process_multi_runs",
            "value": 19009000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_no_prune",
            "value": 8155500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/incremental_with_prune",
            "value": 8216500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "online_learning/learn_and_predict_mixed",
            "value": 8136799,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/5",
            "value": 11234000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/5",
            "value": 180,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/5",
            "value": 3512700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/10",
            "value": 21276000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/10",
            "value": 161,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/10",
            "value": 6722000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/50",
            "value": 34950000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/50",
            "value": 370,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/50",
            "value": 7714400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/train/100",
            "value": 37147000,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/predict/100",
            "value": 154,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "alphabet_scaling/online_update/100",
            "value": 8131100,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_regular",
            "value": 2151500,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/update_burst",
            "value": 2097400,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "hawkes_intensity/boost_computation",
            "value": 872880,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/record_and_predict",
            "value": 4548700,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "conformal_calibration/predict_only",
            "value": 3779,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}