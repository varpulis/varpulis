# Forecasting Tutorial

## What Is Pattern Forecasting?

Standard complex event processing detects patterns *after* they complete. Forecasting goes further: it predicts whether a partially-matched pattern will complete, and estimates when.

For example, a fraud detection pattern might be:

```
Login -> PasswordChange -> LargeTransaction
```

With forecasting, after observing `Login -> PasswordChange`, the engine can estimate the probability that `LargeTransaction` will follow, *before* it happens. This enables proactive intervention rather than reactive alerting.

Varpulis implements forecasting using Prediction Suffix Trees (PST) combined with the SASE NFA to form a Pattern Markov Chain (PMC). The model learns transition probabilities from the live event stream and applies them to the pattern structure.

## Basic `.forecast()` Syntax

The `.forecast()` operator is attached to a sequence pattern, after `.within()`:

```vpl
stream AlertName = EventA as a
    -> EventB where condition as b
    -> EventC where condition as c
    .within(time_window)
    .forecast(confidence: threshold, horizon: duration, warmup: count)
    .where(forecast_probability > threshold)
    .emit(fields...)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `confidence` | float | `0.5` | Minimum probability to emit a forecast |
| `horizon` | duration | `5m` | How far ahead to forecast |
| `warmup` | integer | `100` | Events to observe before forecasting starts |

### Forecast Fields

After `.forecast()`, two new fields become available for use in `.where()` and `.emit()`:

- **`forecast_probability`**: a value between 0.0 and 1.0 representing the estimated probability that the pattern will complete
- **`forecast_expected_time`**: estimated time until pattern completion (in the same duration format as `.within()`)

## Step-by-Step Example

### Step 1: Define the Pattern

Start with a standard sequence pattern. Here we detect a suspicious login sequence:

```vpl
event Login:
    user_id: str
    ip_address: str
    country: str
    ts: timestamp

event PasswordChange:
    user_id: str
    ts: timestamp

event LargeTransfer:
    user_id: str
    amount: float
    destination: str
    ts: timestamp
```

### Step 2: Add the Sequence

```vpl
stream SuspiciousSequence = Login as login
    -> PasswordChange where user_id == login.user_id as pwd_change
    -> LargeTransfer where user_id == login.user_id and amount > 50000 as transfer
    .within(1h)
```

This matches the full three-step pattern within one hour.

### Step 3: Add Forecasting

```vpl
stream FraudForecast = Login as login
    -> PasswordChange where user_id == login.user_id as pwd_change
    -> LargeTransfer where user_id == login.user_id and amount > 50000 as transfer
    .within(1h)
    .forecast(confidence: 0.6, horizon: 30m, warmup: 1000)
```

Now the engine will:
1. Wait for 1,000 events to learn the event stream's statistical properties (warmup)
2. For each partially-matched pattern, compute the probability of completion
3. Only emit forecasts where the probability exceeds 0.6

### Step 4: Filter and Emit

```vpl
stream FraudForecast = Login as login
    -> PasswordChange where user_id == login.user_id as pwd_change
    -> LargeTransfer where user_id == login.user_id and amount > 50000 as transfer
    .within(1h)
    .forecast(confidence: 0.6, horizon: 30m, warmup: 1000)
    .where(forecast_probability > 0.75)
    .emit(
        alert_type: "FRAUD_FORECAST",
        user_id: login.user_id,
        probability: forecast_probability,
        expected_time: forecast_expected_time,
        login_country: login.country,
        severity: if forecast_probability > 0.9 then "critical" else "high"
    )
```

The `.where(forecast_probability > 0.75)` acts as a secondary filter on top of the `confidence: 0.6` threshold, allowing you to separate low-confidence from high-confidence forecasts.

## Tuning Parameters

### Confidence Threshold

The `confidence` parameter controls the minimum probability for emitting any forecast at all. Lower values produce more forecasts (higher recall) but more false positives. Higher values produce fewer, more reliable forecasts.

Recommended starting points:
- **Exploratory/monitoring**: `0.3` -- see what the model predicts
- **Alerting**: `0.5` -- reasonable balance
- **Automated action**: `0.8` -- high confidence required

### Horizon

The `horizon` parameter limits how far ahead the model looks. A longer horizon allows earlier detection but may reduce accuracy because the model must predict further into the future.

Guidelines:
- Set horizon to roughly half the `.within()` window for balanced results
- For time-sensitive patterns, use a shorter horizon (e.g., `horizon: 5m` for a `.within(10m)` pattern)
- The model naturally becomes less confident at longer horizons

### Warmup Period

The `warmup` parameter controls how many events the PST observes before it starts making predictions. During warmup, the model learns the event stream's statistical properties -- no forecasts are emitted.

Guidelines:
- **Low-volume streams** (< 100 events/min): `warmup: 200`
- **Medium-volume streams** (100-10K events/min): `warmup: 500`
- **High-volume streams** (> 10K events/min): `warmup: 1000`

More warmup events produce a better-calibrated model, but delay the start of forecasting. In practice, 500-1000 events is sufficient for most workloads.

## Working With Forecast Results

### Tiered Alerting

Use multiple filters on `forecast_probability` to create tiered alert levels:

```vpl
# High-confidence: automated block
stream AutoBlock = Login as login
    -> PasswordChange where user_id == login.user_id
    -> LargeTransfer where user_id == login.user_id and amount > 50000
    .within(1h)
    .forecast(confidence: 0.5, horizon: 30m, warmup: 500)
    .where(forecast_probability > 0.9)
    .emit(
        action: "AUTO_BLOCK",
        user_id: login.user_id,
        probability: forecast_probability
    )

# Medium-confidence: human review
stream ReviewQueue = Login as login
    -> PasswordChange where user_id == login.user_id
    -> LargeTransfer where user_id == login.user_id and amount > 50000
    .within(1h)
    .forecast(confidence: 0.5, horizon: 30m, warmup: 500)
    .where(forecast_probability > 0.6 and forecast_probability <= 0.9)
    .emit(
        action: "HUMAN_REVIEW",
        user_id: login.user_id,
        probability: forecast_probability,
        expected_time: forecast_expected_time
    )
```

### Using Expected Time

The `forecast_expected_time` field estimates when the pattern will complete. Use it to prioritize alerts or set SLA timers:

```vpl
stream UrgentForecast = SensorReading as baseline
    -> SensorReading where sensor_id == baseline.sensor_id and value > baseline.value * 2 as spike
    -> SystemFailure where system_id == baseline.sensor_id as failure
    .within(30m)
    .forecast(confidence: 0.5, horizon: 15m, warmup: 500)
    .where(forecast_probability > 0.7)
    .emit(
        alert_type: if forecast_expected_time < 5m then "URGENT" else "WARNING",
        sensor_id: baseline.sensor_id,
        probability: forecast_probability,
        minutes_remaining: forecast_expected_time
    )
```

## Full Example: IoT Predictive Maintenance

```vpl
connector SensorMQTT = mqtt(
    host: "iot-gateway.local",
    port: 1883,
    client_id: "forecaster"
)

event VibrationReading:
    machine_id: str
    amplitude: float
    frequency: float
    ts: timestamp

event TemperatureSpike:
    machine_id: str
    temp_celsius: float
    ts: timestamp

event MachineFailure:
    machine_id: str
    error_code: str
    ts: timestamp

# Forecast: vibration anomaly -> temp spike -> failure
stream PredictiveMaintenanceAlert = VibrationReading as vib
    -> TemperatureSpike where machine_id == vib.machine_id and temp_celsius > 80 as temp
    -> MachineFailure where machine_id == vib.machine_id as failure
    .within(2h)
    .forecast(confidence: 0.5, horizon: 1h, warmup: 1000)
    .where(forecast_probability > 0.6)
    .emit(
        alert_type: "PREDICTED_FAILURE",
        machine_id: vib.machine_id,
        probability: forecast_probability,
        expected_in: forecast_expected_time,
        vibration_amplitude: vib.amplitude,
        temperature: temp.temp_celsius,
        recommendation: if forecast_probability > 0.8
            then "Schedule immediate maintenance"
            else "Monitor closely, maintenance may be needed"
    )
```

## How It Works Internally

1. **Pattern Markov Chain (PMC)**: The `.forecast()` operator constructs a PMC from the pattern's NFA (the sequence structure) and a Prediction Suffix Tree (PST) that learns from the live stream.

2. **Online Learning**: As events flow through the engine, the PST continuously updates its model of event transition probabilities. It uses variable-order Markov modeling -- deeper context where it helps, shallower where it does not.

3. **Forward Algorithm**: When a partial match is active (e.g., `Login` matched, waiting for `PasswordChange`), the PMC runs a forward computation over the product of NFA states and PST-predicted transition probabilities to estimate completion probability.

4. **Adaptive Pruning**: The PST periodically prunes nodes whose distributions do not differ significantly from their parent, keeping the model compact and fast.

5. **Smoothing**: Laplace smoothing ensures all event types have non-zero probability, preventing the model from assigning zero probability to unseen transitions.
