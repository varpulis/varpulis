# Varpulis Real-World Scenarios

Five industry scenarios demonstrating Varpulis's key differentiators: native Kleene closure, Hamlet multi-query optimization, sub-millisecond latency, and 3-16x memory advantage.

| Scenario | Industry | Key Differentiator | Document |
|----------|----------|--------------------|----------|
| [Credit Card Fraud Detection](fraud-detection.md) | Financial Services | Kleene closure captures 5x more fraud patterns than greedy-matching competitors | [VPL](../../tests/scenarios/cxo_fraud_detection.vpl) |
| [Predictive Equipment Failure](predictive-maintenance.md) | Manufacturing / IoT | Multi-sensor correlation with explainable alerts | [VPL](../../tests/scenarios/cxo_predictive_maintenance.vpl) |
| [Insider Trading Surveillance](insider-trading.md) | Capital Markets | Hamlet trend_aggregate monitors 50+ symbols at 100x throughput | [VPL](../../tests/scenarios/cxo_insider_trading.vpl) |
| [Cyber Kill Chain Detection](cyber-kill-chain.md) | Cybersecurity / SOC | Multi-stage attack detection with complete forensic trail | [VPL](../../tests/scenarios/cxo_cyber_threat.vpl) |
| [Patient Safety Monitoring](patient-safety.md) | Healthcare | Sub-millisecond drug interaction detection across prescribers | [VPL](../../tests/scenarios/cxo_patient_safety.vpl) |

## Running the Scenarios

Each scenario has a `.vpl` (rules) and `.evt` (test events) file in `tests/scenarios/`.

```bash
# Run all CxO scenario tests
cargo test -p varpulis-runtime --test cxo_scenario_tests

# Run a specific scenario
cargo test -p varpulis-runtime --test cxo_scenario_tests cxo_fraud

# Run with a live event file
varpulis simulate tests/scenarios/cxo_fraud_detection.vpl \
    --events tests/scenarios/cxo_fraud_detection.evt
```

## Benchmark Highlights

| Metric | Varpulis | Traditional CEP | Advantage |
|--------|----------|----------------|-----------|
| Single-query throughput | 6.9 M events/s | 2.4 M events/s | **3x** |
| Multi-query (50 patterns) | 950 K events/s | 9 K events/s | **100x** |
| Memory per instance | 10-54 MB | 85-190 MB | **3-16x less** |
| Latency | Sub-millisecond | 15-minute batch cycles | **Real-time** |
