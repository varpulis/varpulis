import type { ScenarioDefinition } from '@/types/scenario'

export const patientSafetyScenario: ScenarioDefinition = {
  id: 'patient-safety',
  title: 'Patient Safety Monitoring',
  subtitle: 'Healthcare',
  icon: 'mdi-hospital-box',
  color: 'teal',
  summary:
    'Medical errors are the 3rd leading cause of death. Detect drug interactions, vital sign deterioration, and dosage anomalies in real-time.',
  vplSource: `stream DrugInteraction = Prescription as rx1
    -> Prescription where patient_id == rx1.patient_id as rx2
    .within(24h)
    .where(rx2.drug_class == rx1.interacts_with)
    .emit(
        alert_type: "drug_interaction",
        patient_id: rx1.patient_id,
        drug1: rx1.drug_name,
        drug2: rx2.drug_name,
        prescriber1: rx1.doctor_id,
        prescriber2: rx2.doctor_id
    )

stream VitalDeterioration = VitalSign as reading1
    -> VitalSign where patient_id == reading1.patient_id as reading2
    .within(4h)
    .where(reading2.heart_rate > reading1.heart_rate * 1.2)
    .emit(
        alert_type: "vital_deterioration",
        patient_id: reading1.patient_id,
        heart_rate_initial: reading1.heart_rate,
        heart_rate_current: reading2.heart_rate
    )

stream DosageAnomaly = Prescription as rx
    .where(dosage_mg > max_safe_mg)
    .emit(
        alert_type: "dosage_anomaly",
        patient_id: rx.patient_id,
        drug_name: rx.drug_name,
        prescribed_mg: rx.dosage_mg,
        max_safe_mg: rx.max_safe_mg
    )`,
  patterns: [
    {
      name: 'Drug Interaction',
      description:
        'Detects two prescriptions for the same patient within 24 hours where the second drug class matches the first drug\'s interaction warning. Catches dangerous combinations prescribed by different doctors.',
      vplSnippet: `Prescription as rx1
    -> Prescription where patient_id == rx1.patient_id as rx2
    .within(24h)
    .where(rx2.drug_class == rx1.interacts_with)`,
    },
    {
      name: 'Vital Sign Deterioration',
      description:
        'Detects heart rate increasing by more than 20% between two readings for the same patient within 4 hours. With 6 escalating readings, every earlier-to-later pair that exceeds the 20% threshold produces a match.',
      vplSnippet: `VitalSign as reading1
    -> VitalSign where patient_id == reading1.patient_id as reading2
    .within(4h)
    .where(reading2.heart_rate > reading1.heart_rate * 1.2)`,
    },
    {
      name: 'Dosage Anomaly',
      description:
        'Detects a prescription where the prescribed dosage exceeds the known maximum safe dosage for that drug. Immediate safety check on every prescription event.',
      vplSnippet: `Prescription as rx
    .where(dosage_mg > max_safe_mg)`,
    },
  ],
  steps: [
    {
      title: 'Normal Vitals',
      narration:
        'Three patients have stable vital signs over an hour. Heart rates change by only 1-3 bpm, well below the 20% deterioration threshold. Prescriptions are within safe dosage limits. Zero alerts expected.',
      eventsText: `VitalSign { patient_id: "P-101", heart_rate: 72.0, spo2: 98.0, bp_systolic: 118 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 80.0, spo2: 97.0, bp_systolic: 125 }
BATCH 600000
VitalSign { patient_id: "P-303", heart_rate: 68.0, spo2: 99.0, bp_systolic: 115 }
BATCH 1200000
VitalSign { patient_id: "P-101", heart_rate: 74.0, spo2: 98.0, bp_systolic: 120 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 82.0, spo2: 97.0, bp_systolic: 126 }
BATCH 600000
Prescription { patient_id: "P-404", drug_name: "ibuprofen", drug_class: "nsaid", interacts_with: "none", dosage_mg: 400.0, max_safe_mg: 800.0, doctor_id: "DR-D" }
BATCH 600000
VitalSign { patient_id: "P-303", heart_rate: 70.0, spo2: 99.0, bp_systolic: 116 }
BATCH 600000
Prescription { patient_id: "P-101", drug_name: "metformin", drug_class: "antidiabetic", interacts_with: "none", dosage_mg: 500.0, max_safe_mg: 2000.0, doctor_id: "DR-A" }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Drug Interaction',
      narration:
        'Patient P-101 is prescribed warfarin (anticoagulant) by Dr. A, then 6 hours later Dr. B prescribes aspirin (antiplatelet). Warfarin lists "antiplatelet" as its interaction class. Meanwhile, safe prescriptions for P-202 and P-303 arrive without triggering alerts.',
      eventsText: `Prescription { patient_id: "P-101", drug_name: "warfarin", drug_class: "anticoagulant", interacts_with: "antiplatelet", dosage_mg: 5.0, max_safe_mg: 10.0, doctor_id: "DR-A" }
BATCH 7200000
Prescription { patient_id: "P-202", drug_name: "lisinopril", drug_class: "ace_inhibitor", interacts_with: "none", dosage_mg: 10.0, max_safe_mg: 40.0, doctor_id: "DR-C" }
BATCH 3600000
Prescription { patient_id: "P-303", drug_name: "omeprazole", drug_class: "ppi", interacts_with: "none", dosage_mg: 20.0, max_safe_mg: 40.0, doctor_id: "DR-D" }
BATCH 7200000
Prescription { patient_id: "P-101", drug_name: "aspirin", drug_class: "antiplatelet", interacts_with: "anticoagulant", dosage_mg: 325.0, max_safe_mg: 1000.0, doctor_id: "DR-B" }
BATCH 3600000
Prescription { patient_id: "P-202", drug_name: "amlodipine", drug_class: "calcium_blocker", interacts_with: "none", dosage_mg: 5.0, max_safe_mg: 10.0, doctor_id: "DR-C" }`,
      expectedAlerts: ['drug_interaction'],
      phase: 'attack',
    },
    {
      title: 'Vital Deterioration',
      narration:
        'Patient P-202 heart rate climbs from 88 to 142 bpm over 6 readings, interleaved with stable readings from P-303 (68-70 bpm). Every earlier-to-later pair that exceeds the 20% threshold fires a vital_deterioration alert \u2014 multiple matches from the escalating sequence.',
      eventsText: `VitalSign { patient_id: "P-202", heart_rate: 88.0, spo2: 96.0, bp_systolic: 120 }
BATCH 600000
VitalSign { patient_id: "P-303", heart_rate: 68.0, spo2: 99.0, bp_systolic: 115 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 95.0, spo2: 94.0, bp_systolic: 128 }
BATCH 600000
VitalSign { patient_id: "P-303", heart_rate: 69.0, spo2: 99.0, bp_systolic: 116 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 108.0, spo2: 92.0, bp_systolic: 135 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 118.0, spo2: 90.0, bp_systolic: 140 }
BATCH 600000
VitalSign { patient_id: "P-303", heart_rate: 70.0, spo2: 98.0, bp_systolic: 117 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 130.0, spo2: 88.0, bp_systolic: 148 }
BATCH 600000
VitalSign { patient_id: "P-303", heart_rate: 69.0, spo2: 99.0, bp_systolic: 115 }
BATCH 600000
VitalSign { patient_id: "P-202", heart_rate: 142.0, spo2: 85.0, bp_systolic: 155 }`,
      expectedAlerts: ['vital_deterioration'],
      phase: 'attack',
    },
    {
      title: 'Dosage Anomaly + Safe Comparison',
      narration:
        'Acetaminophen 1500mg prescribed for P-303 exceeds the 1000mg safe maximum \u2014 dosage_anomaly fires immediately. A second prescription of 500mg acetaminophen for P-404 is within safe limits and produces no alert.',
      eventsText: `Prescription { patient_id: "P-303", drug_name: "acetaminophen", drug_class: "analgesic", interacts_with: "none", dosage_mg: 1500.0, max_safe_mg: 1000.0, doctor_id: "DR-C" }
BATCH 5000
Prescription { patient_id: "P-404", drug_name: "acetaminophen", drug_class: "analgesic", interacts_with: "none", dosage_mg: 500.0, max_safe_mg: 1000.0, doctor_id: "DR-D" }`,
      expectedAlerts: ['dosage_anomaly'],
      phase: 'attack',
    },
  ],
}
