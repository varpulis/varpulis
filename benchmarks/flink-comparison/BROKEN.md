# This harness does not measure Varpulis

**Do not quote figures from `run_all_scenarios.sh` until this is fixed.**

None of the five scenario VPL files has a `.from(...)` source binding:

```
$ grep -c '\.from(' benchmarks/flink-comparison/scenario*/varpulis.vpl
scenario1-aggregation/varpulis.vpl:0
scenario2-sequence/varpulis.vpl:0
scenario3-fraud/varpulis.vpl:0
scenario4-join/varpulis.vpl:0
scenario5-anomaly/varpulis.vpl:0
```

`varpulis run` on a program with no source binding prints *"No source connector
bindings found"* and idles. The Flink job on the other side of the comparison
runs for real, so every latency figure this harness produced compares a working
Flink job against a Varpulis process that ingested nothing.

The correctness gate cannot catch it either: `run_all_scenarios.sh:232` checks
output with `grep -c "emit\|alert\|ALERT\|published"` over stdout, which matches
the word `emit` in the program's own echoed source.

## To fix

1. Give each scenario a real source binding and sink, against the same broker
   and topics the Flink job uses.
2. Replace the `grep` gate with an output-count assertion, as
   `benchmarks/arroyo-comparison/` does.
3. Re-measure, and only then restore any figure to the docs.

Until then this directory is kept for the Flink jobs and the datasets, which are
sound; the Varpulis half is not.
