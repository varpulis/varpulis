# Forecasting Architecture

## Overview

Varpulis implements pattern forecasting based on the Prediction Suffix Tree (PST) combined with the SASE NFA to form a Pattern Markov Chain (PMC). This enables the engine to predict whether a partially-matched complex event pattern will complete, and estimate when.

Reference: *Complex Event Forecasting with Prediction Suffix Trees* (Alevizos, Artikis, Paliouras -- arXiv:2109.00287)

## Prediction Suffix Tree (PST)

The PST is a variable-order Markov model that learns conditional probability distributions over event types from the observed event stream. Unlike fixed-order Markov models, the PST adapts context depth to the data, using deeper context only where it provides statistically significant predictive power.

### Structure

Each node in the tree represents a context (suffix of recent event types). The root represents the empty context (order-0 Markov). Children represent progressively longer contexts up to `max_depth`.

```
root (empty context)
├── A → P(next | ..., A)
│   ├── B,A → P(next | ..., B, A)
│   └── C,A → P(next | ..., C, A)
├── B → P(next | ..., B)
│   └── A,B → P(next | ..., A, B)
└── C → P(next | ..., C)
```

At each node, the tree stores:
- **Counts**: how many times each next-symbol was observed after this context
- **Total**: total observations at this node
- **Smoothed probability**: `P(s | context) = (count(s) + alpha) / (total + alpha * |alphabet|)`

Laplace smoothing (parameter `alpha`) ensures all symbols have non-zero probability, preventing zero-probability transitions in the PMC.

### Training

Batch training iterates over the input sequence. For each position `i`, it updates all suffix nodes of lengths `0..max_depth` ending just before position `i`, incrementing the count for `sequence[i]`.

Time complexity: `O(n * d)` where `n` is sequence length and `d` is `max_depth`.

### Prediction

Given a context (recent event history), the PST walks the tree to find the longest matching suffix. It returns the smoothed probability distribution at that node. If the full context is not in the tree, it falls back to the longest available prefix, providing graceful degradation.

## Pattern Markov Chain (PMC)

The PMC combines the PST with the SASE NFA to forecast pattern completion. The NFA defines the structure of the pattern being matched (states, transitions, accept states), while the PST provides transition probabilities learned from the stream.

### Construction

```
PMC = PST x NFA
```

The PMC is constructed from:
1. **NFA transition structure**: `state -> { symbol -> next_state }`
2. **Accept states**: the final states of the NFA
3. **PST**: provides `P(symbol | context)` for each transition
4. **Configuration**: confidence threshold, forecast horizon, warmup period

### Forward Algorithm for Completion Probability

To compute the probability that a pattern will complete from the current NFA state, the PMC uses a value iteration approach on the product of the PST context and NFA states:

1. Initialize: `P(accept_state) = 1.0`, all others `0.0`
2. Iterate: for each non-accept state `s`, compute:
   ```
   P(s) = sum over transitions (s, symbol, s'):
       P(symbol | PST_context) * P(s')
   ```
3. Converge after `max_simulation_steps` iterations (default: 50)
4. Return `P(current_state)` clamped to `[0.0, 1.0]`

This gives a single scalar: the probability that the partially-matched pattern will eventually complete.

### Waiting Time Estimation

Expected time to completion is estimated by:

1. BFS from current NFA state to find minimum transitions to accept
2. Divide by the sum of PST-predicted transition probabilities (progress rate)
3. Multiply by the exponentially-smoothed average inter-event time

```
expected_time = (min_transitions / progress_probability) * avg_inter_event_ns
```

### Event Processing Pipeline

When `PMC.process()` is called with a new event:

1. **Update inter-event timing** (exponential moving average, alpha=0.05)
2. **Update PST online** via the `OnlinePSTLearner`
3. **Suppress during warmup** (configurable, default 100 events)
4. **Select best active run** (most advanced NFA state)
5. **Compute completion probability** via forward algorithm
6. **Estimate waiting time** via BFS + probability weighting
7. **Track outcomes**: compare active runs to previous snapshot; disappeared runs feed the conformal calibrator
8. **Compute Hawkes-modulated probability** via forward algorithm with intensity boosting
9. **Compute conformal interval**: `(lower, upper)` from calibration history
10. **Return `ForecastResult`** with probability, expected time, state label, context depth, lower, upper

## Hawkes Process Intensity Modulation

The PST alone is temporally blind: it sees only symbol order, not timing density. A burst of 5 vibration events in 10 seconds and 1 event per hour look identical. The Hawkes process tracks temporal intensity for each event type, boosting completion probability when relevant events cluster in time.

### Self-Exciting Point Process

Each event type has an independent `HawkesIntensity` tracker:

```
intensity(t) = mu + (intensity(t_prev) - mu + alpha) * exp(-beta * dt)
```

- **mu**: baseline rate (events/ns), estimated online from mean inter-event time
- **alpha**: excitation magnitude (fraction of mu), how much each event raises intensity
- **beta**: decay rate (1/ns), inversely proportional to inter-event time variance
- **boost_factor**: `clamp(intensity / mu, 1.0, 5.0)`

The update is O(1) per event via the recursive formula. Parameters are re-estimated every 50 events using moment matching after a minimum of 10 observations.

### Integration with PMC

In `compute_completion_probability()`, each PST transition probability is modulated by the Hawkes boost factor for that symbol's event type:

```
modulated_prob(symbol) = pst_prob(symbol) * hawkes_boost(symbol, now_ns)
```

The modulated probabilities are renormalized per-state to maintain valid transition distributions, then scaled by the original PST total to preserve the overall probability magnitude.

## Conformal Prediction Intervals

Raw probability forecasts like `0.73` provide no indication of confidence. The conformal calibrator produces distribution-free `[lower, upper]` bounds with formal 90% coverage guarantees.

### Nonconformity Scores

For each past forecast, a nonconformity score records the prediction error:

```
score = |predicted_probability - actual_outcome|
```

where `actual_outcome` is 1.0 (pattern completed) or 0.0 (pattern expired/aborted).

### Outcome Tracking

The PMC detects outcomes by comparing the current set of active SASE runs to the previous snapshot:
- **Disappeared run in accept state** -> pattern completed (outcome = 1.0)
- **Disappeared run not in accept state** -> pattern expired (outcome = 0.0)

Each outcome is paired with its stored forecast probability to produce a calibration score.

### Prediction Interval

Given a predicted probability `p` and the empirical quantile `q` at coverage level `1 - alpha`:

```
interval = [max(0, p - q), min(1, p + q)]
```

The quantile is computed from a sliding window of 1000 recent scores using the standard conformal prediction formula: `index = ceil((n+1) * alpha)`. Before calibration data is available, the interval defaults to `[0.0, 1.0]` (maximum uncertainty). As the engine observes outcomes, the interval narrows.

## Online Learning

The `OnlinePSTLearner` maintains a rolling context buffer and incrementally updates the PST as new events arrive. This allows the model to adapt to non-stationary event distributions without full retraining.

### Incremental Updates

For each new symbol:
1. Update counts at the root and all context-suffix nodes (lengths `0..max_depth`)
2. Create new tree nodes as needed (lazy expansion)
3. Append symbol to context buffer, bounded to `max_context` length

### Pruning

Periodic pruning keeps the tree compact by removing nodes whose conditional distribution is not significantly different from their parent. Two strategies are supported:

- **KL Divergence**: prune if `D_KL(child || parent) < threshold` (default: 0.05)
- **Entropy**: prune if entropy reduction from the child node is below `min_reduction`

Pruning is triggered every `prune_interval` updates (default: 10,000). Only leaf nodes are pruned (bottom-up).

## Performance Characteristics

| Operation | Time Complexity | Notes |
|---|---|---|
| PST batch training | O(n * d) | n=sequence length, d=max_depth |
| PST prediction | O(d) | Context walk, d=max_depth |
| Online update | O(d^2) | Update d suffix nodes, each up to d steps |
| Completion probability | O(S * T * I) | S=states, T=transitions, I=iterations |
| Waiting time | O(S + T) | BFS traversal |
| Pruning | O(N * K) | N=nodes, K=alphabet size for KL computation |
| Hawkes update | O(1) | Recursive intensity formula per event |
| Conformal interval | O(W log W) | W=window size (1000), sort for quantile |
| Outcome tracking | O(R) | R=number of active runs |

Memory usage scales with `O(|alphabet|^d)` in the worst case (all possible contexts observed), but pruning and the variable-order property keep the actual tree much smaller in practice.

### Typical Throughput

With a 10-symbol alphabet and depth 5:
- Batch training: ~5M symbols/sec
- Online updates: ~2M updates/sec
- Prediction: ~10M predictions/sec (single symbol)
- PMC forecast (3-state NFA): ~500K events/sec including online learning

## Module Structure

```
crates/varpulis-runtime/src/pst/
├── mod.rs            # Public re-exports
├── tree.rs           # PredictionSuffixTree, PSTNode, PSTConfig
├── markov_chain.rs   # PatternMarkovChain, PMCConfig, ForecastResult, RunSnapshot
├── online.rs         # OnlinePSTLearner (incremental updates)
├── pruning.rs        # PruningStrategy (KL divergence, entropy)
├── hawkes.rs         # HawkesIntensity (self-exciting temporal density tracker)
└── conformal.rs      # ConformalCalibrator (distribution-free prediction intervals)
```

## VPL Integration

In VPL, forecasting is attached to sequence patterns via the `.forecast()` operator:

```vpl
# Industrial predictive maintenance: predict bearing failure from sensor cascade
stream BearingFailureAlert = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value_celsius > 75 as temp
    -> MachineFailure where machine_id == vib.machine_id as failure
    .within(4h)
    .forecast(confidence: 0.5, horizon: 2h, warmup: 500)
    .where(forecast_probability > 0.7)
    .emit(
        alert_type: "PREDICTED_BEARING_FAILURE",
        machine_id: vib.machine_id,
        probability: forecast_probability,
        expected_time: forecast_expected_time,
        recommendation: if forecast_probability > 0.85
            then "Schedule emergency bearing replacement"
            else "Monitor closely, prepare replacement parts"
    )
```

The `.forecast()` operator:
1. Constructs a PMC from the pattern's NFA and the configured PST
2. Runs online learning on every incoming event
3. Emits `forecast_probability` and `forecast_expected_time` fields
4. The subsequent `.where()` can filter on these fields to suppress low-confidence predictions

### Why Forecasting Fits Industrial and Security Domains

PST-based forecasting is most effective when:
- **Sequences are physics-constrained or procedurally ordered** — vibration always precedes thermal failure; reconnaissance always precedes exploitation
- **Intervention windows are wide** — hours to days for equipment, hours to weeks for cyber intrusions
- **The cost of missing a prediction is high** — unplanned downtime ($100K-$300K/hr in heavy industry) or data exfiltration
- **Variable-order context matters** — the same temperature spike means different things after a vibration anomaly vs. after routine startup
