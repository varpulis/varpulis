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
7. **Return `ForecastResult`** with probability, expected time, state label, context depth

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
└── pruning.rs        # PruningStrategy (KL divergence, entropy)
```

## VPL Integration

In VPL, forecasting is attached to sequence patterns via the `.forecast()` operator:

```vpl
stream FraudAlert = Login as login
    -> PasswordChange where user_id == login.user_id
    -> Transaction where user_id == login.user_id and amount > 10000
    .within(30m)
    .forecast(confidence: 0.7, horizon: 5m, warmup: 500)
    .where(forecast_probability > 0.8)
    .emit(
        alert_type: "LIKELY_FRAUD",
        probability: forecast_probability,
        expected_time: forecast_expected_time
    )
```

The `.forecast()` operator:
1. Constructs a PMC from the pattern's NFA and the configured PST
2. Runs online learning on every incoming event
3. Emits `forecast_probability` and `forecast_expected_time` fields
4. The subsequent `.where()` can filter on these fields to suppress low-confidence predictions
