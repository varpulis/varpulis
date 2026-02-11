# Patient Safety Monitoring

## Executive Summary

Medical errors cause 250,000 deaths per year in the US. Varpulis monitors prescriptions and vital signs in real-time, catching dangerous drug interactions and deteriorating patients *before* harm occurs -- something batch-processing hospital systems fundamentally cannot do.

## The Business Problem

Hospital IT systems were built for record-keeping, not real-time safety. When Doctor A prescribes warfarin (a blood thinner) at 8 AM and Doctor B prescribes aspirin (also a blood thinner) at 2 PM for the *same patient*, the dangerous interaction may not be flagged until the 3 PM batch run -- or the next morning's pharmacy review. By then, the nurse may have already administered both drugs.

Three categories of preventable harm:
- **Drug interactions:** Different doctors prescribe conflicting medications for the same patient, each unaware of the other's prescription
- **Vital sign deterioration:** A patient's condition worsens gradually over hours, with each individual reading just barely within "normal" range
- **Dosage errors:** A prescription exceeds the safe maximum dose, often due to unit confusion or decimal point errors

## How It Works

Varpulis processes each medical event as it occurs -- prescriptions, vital sign readings, lab results -- and matches them against safety patterns in real-time:

```
Drug Interaction Detection:

  8:00 AM           2:00 PM
     │                  │
     ▼                  ▼
┌──────────┐     ┌──────────┐
│ Warfarin │────►│ Aspirin  │──► ALERT!
│ (Dr. A)  │     │ (Dr. B)  │
│ P-101    │     │ P-101    │
└──────────┘     └──────────┘
anticoagulant    antiplatelet

Same patient, interacting drug classes
Alert reaches nurse station in < 1 ms
```

## What Varpulis Detects

### Drug Interaction
Doctor A prescribes warfarin (blood thinner, class: anticoagulant) at 8 AM. Doctor B prescribes aspirin (also a blood thinner, class: antiplatelet) at 2 PM -- different doctors, same patient. Together, these drugs cause dangerous internal bleeding. Varpulis catches the interaction the instant the second prescription is entered, before the medication leaves the pharmacy.

### Vital Sign Deterioration
A patient's heart rate goes from 90 to 135 bpm over one hour -- a 50% increase. That crosses the 20% deterioration threshold. Each individual reading might not trigger a simple threshold alarm (both are "within normal range" for some patients). But the *trend* -- a significant increase over a short period -- is a clear sign of deterioration.

### Dosage Anomaly
A prescription for 1,500 mg of acetaminophen where the maximum safe dose is 1,000 mg. A simple threshold check, but life-saving. Varpulis flags it instantly -- not at the next batch review.

## Why Competitors Miss This

Most hospital IT systems process data in batches -- every 15 minutes, every hour, or when a nurse manually checks. A dangerous drug interaction prescribed at 2:01 PM will not be flagged until the 2:15 batch run. By then, the nurse may have already administered the drug.

More critically, batch systems only check within their own data silo. The pharmacy system knows about prescriptions but not vital signs. The monitoring system knows vital signs but not medications. Varpulis processes *all* event types in a single stream, correlating prescriptions from different doctors with real-time vital sign changes.

| Capability | Batch Systems | Varpulis |
|-----------|--------------|----------|
| Alert latency | 15 min - 1 hour | < 1 millisecond |
| Cross-prescriber detection | Next-day review | Instant |
| Vital sign trends | Threshold only | Sequence patterns |
| Cross-silo correlation | Manual | Automatic |

## Measurable Impact

- **Sub-millisecond alert latency** -- alert reaches the nurse's station before they open the medication cabinet
- **Cross-prescriber interaction detection** -- different doctors, same patient, caught instantly
- **Trend-based vital sign monitoring** -- catches deterioration that single-threshold alarms miss
- **Zero missed dosage anomalies** -- every prescription checked against safe limits in real-time

## Live Demo Walkthrough

```bash
cargo test -p varpulis-runtime --test cxo_scenario_tests cxo_patient
```

**Input events:**
1. Warfarin prescribed to P-101 by Dr. A (anticoagulant)
2. Aspirin prescribed to P-101 by Dr. B (antiplatelet -- interacts with anticoagulant)
3. P-202 heart rate: 90 bpm (baseline)
4. P-202 heart rate: 135 bpm (50% increase)
5. P-303 prescribed 1,500 mg acetaminophen (max safe: 1,000 mg)
6. P-404 normal vital signs (healthy patient)

**Alerts generated:**
- `drug_interaction` -- P-101, warfarin (Dr. A) + aspirin (Dr. B)
- `vital_deterioration` -- P-202, heart rate 90 to 135 bpm
- `dosage_anomaly` -- P-303, 1,500 mg > 1,000 mg max

**No false positives:** Patient P-404 with stable vital signs generates zero alerts.

<details>
<summary>Technical Appendix</summary>

### VPL Pattern: Drug Interaction
```
stream DrugInteraction = Prescription as rx1
    -> Prescription where patient_id == rx1.patient_id as rx2
    .within(24h)
    .where(rx2.drug_class == rx1.interacts_with)
    .emit(alert_type: "drug_interaction", ...)
```

### VPL Pattern: Vital Sign Deterioration
```
stream VitalDeterioration = VitalSign as reading1
    -> VitalSign where patient_id == reading1.patient_id as reading2
    .within(4h)
    .where(reading2.heart_rate > reading1.heart_rate * 1.2)
    .emit(alert_type: "vital_deterioration", ...)
```

### Test Files
- Rules: `tests/scenarios/cxo_patient_safety.vpl`
- Events: `tests/scenarios/cxo_patient_safety.evt`
</details>
