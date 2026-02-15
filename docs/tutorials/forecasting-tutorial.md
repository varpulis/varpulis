# Forecasting Tutorial

## What Is Pattern Forecasting?

Standard complex event processing detects patterns *after* they complete. Forecasting goes further: it predicts whether a partially-matched pattern will complete, and estimates when.

For example, an industrial equipment failure pattern might be:

```
VibrationAnomaly -> TemperatureRise -> BearingFailure
```

With forecasting, after observing `VibrationAnomaly -> TemperatureRise`, the engine can estimate the probability that `BearingFailure` will follow, *before* it happens. This turns hours of warning into proactive maintenance instead of reactive repair — avoiding unplanned downtime that costs $100K-$300K per hour in heavy industry.

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

## Step-by-Step Example: Industrial Predictive Maintenance

### Step 1: Define the Events

Start with the sensor events that form the failure cascade. In rotating equipment (pumps, turbines, compressors), bearing failure follows a well-known physical sequence: vibration anomaly → temperature rise → mechanical failure.

```vpl
event VibrationReading:
    machine_id: str
    amplitude_mm: float
    frequency_hz: float
    zone: str
    ts: timestamp

event TemperatureReading:
    machine_id: str
    value_celsius: float
    zone: str
    ts: timestamp

event MachineFailure:
    machine_id: str
    failure_type: str
    severity: str
    ts: timestamp
```

### Step 2: Add the Sequence

```vpl
stream BearingFailurePattern = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value_celsius > 75 as temp
    -> MachineFailure where machine_id == vib.machine_id as failure
    .within(4h)
```

This matches the full three-step failure cascade within a 4-hour window. Without forecasting, alerts fire only *after* the failure has already occurred.

### Step 3: Add Forecasting

```vpl
stream BearingFailureForecast = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value_celsius > 75 as temp
    -> MachineFailure where machine_id == vib.machine_id as failure
    .within(4h)
    .forecast(confidence: 0.5, horizon: 2h, warmup: 500)
```

Now the engine will:
1. Wait for 500 events to learn the sensor stream's statistical properties (warmup)
2. For each partially-matched pattern (e.g., vibration seen, waiting for temperature rise), compute the probability of eventual failure
3. Only emit forecasts where the probability exceeds 0.5

### Step 4: Filter and Emit

```vpl
stream BearingFailureForecast = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value_celsius > 75 as temp
    -> MachineFailure where machine_id == vib.machine_id as failure
    .within(4h)
    .forecast(confidence: 0.5, horizon: 2h, warmup: 500)
    .where(forecast_probability > 0.7)
    .emit(
        alert_type: "PREDICTED_BEARING_FAILURE",
        machine_id: vib.machine_id,
        zone: vib.zone,
        probability: forecast_probability,
        expected_time: forecast_expected_time,
        vibration_amplitude: vib.amplitude_mm,
        temperature: temp.value_celsius,
        severity: if forecast_probability > 0.85 then "critical" else "warning",
        recommendation: if forecast_probability > 0.85
            then "Schedule emergency bearing replacement within 1 hour"
            else "Monitor closely and prepare replacement parts"
    )
```

The `.where(forecast_probability > 0.7)` acts as a secondary filter on top of the `confidence: 0.5` threshold, allowing you to separate low-confidence from high-confidence forecasts. A maintenance team can then act on high-probability alerts (>85%) with emergency repairs, while monitoring lower-probability alerts (70-85%) with standby parts ready.

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
# High-confidence: automated shutdown
stream EmergencyShutdown = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value_celsius > 75
    -> MachineFailure where machine_id == vib.machine_id
    .within(4h)
    .forecast(confidence: 0.5, horizon: 2h, warmup: 500)
    .where(forecast_probability > 0.9)
    .emit(
        action: "EMERGENCY_SHUTDOWN",
        machine_id: vib.machine_id,
        probability: forecast_probability
    )

# Medium-confidence: schedule maintenance
stream MaintenanceQueue = VibrationReading as vib
    -> TemperatureReading where machine_id == vib.machine_id and value_celsius > 75
    -> MachineFailure where machine_id == vib.machine_id
    .within(4h)
    .forecast(confidence: 0.5, horizon: 2h, warmup: 500)
    .where(forecast_probability > 0.6 and forecast_probability <= 0.9)
    .emit(
        action: "SCHEDULE_MAINTENANCE",
        machine_id: vib.machine_id,
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

## Full Example: Cybersecurity APT Detection

Advanced Persistent Threats (APTs) follow a predictable kill chain: reconnaissance → initial exploitation → lateral movement → data exfiltration. With dwell times of 56-200 days, there is a wide intervention window — but only if you can predict that a partial sequence will progress.

```vpl
connector SiemKafka = kafka(
    brokers: ["siem-kafka:9092"],
    group_id: "apt-forecaster"
)

event NetworkScan:
    source_ip: str
    target_subnet: str
    ports_scanned: int
    ts: timestamp

event ExploitAttempt:
    source_ip: str
    target_host: str
    cve_id: str
    success: bool
    ts: timestamp

event LateralMovement:
    source_host: str
    target_host: str
    method: str  # "pass_the_hash", "psexec", "wmi", "ssh"
    ts: timestamp

event DataExfiltration:
    source_host: str
    destination_ip: str
    bytes_transferred: int
    protocol: str
    ts: timestamp

# Forecast: scan -> exploit -> lateral move -> exfiltration
stream APTForecast = NetworkScan as scan
    -> ExploitAttempt where source_ip == scan.source_ip and success == true as exploit
    -> LateralMovement where source_host == exploit.target_host as lateral
    -> DataExfiltration where source_host == lateral.target_host as exfil
    .within(7d)
    .forecast(confidence: 0.4, horizon: 3d, warmup: 2000)
    .where(forecast_probability > 0.5)
    .emit(
        alert_type: "APT_KILL_CHAIN_FORECAST",
        severity: if forecast_probability > 0.8 then "critical" else "high",
        source_ip: scan.source_ip,
        compromised_host: exploit.target_host,
        probability: forecast_probability,
        expected_time: forecast_expected_time,
        recommendation: if forecast_probability > 0.75
            then "Isolate compromised host immediately, begin incident response"
            else "Increase monitoring on subnet, block lateral movement paths"
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

3. **Forward Algorithm**: When a partial match is active (e.g., `VibrationAnomaly` matched, waiting for `TemperatureRise`), the PMC runs a forward computation over the product of NFA states and PST-predicted transition probabilities to estimate completion probability.

4. **Adaptive Pruning**: The PST periodically prunes nodes whose distributions do not differ significantly from their parent, keeping the model compact and fast.

5. **Smoothing**: Laplace smoothing ensures all event types have non-zero probability, preventing the model from assigning zero probability to unseen transitions.
