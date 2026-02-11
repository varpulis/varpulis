<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import VitalsMonitor from '@/components/demos/patient/VitalsMonitor.vue'
import MedicationTimeline from '@/components/demos/patient/MedicationTimeline.vue'
import { patientSafetyScenario } from '@/data/scenarios/patient-safety'

interface VitalReading {
  heartRate: number
  spo2: number
}

interface Medication {
  time: string
  drug: string
  dosage: string
  doctor: string
  status: 'safe' | 'danger' | 'warning'
}

const readings = ref<VitalReading[]>([])
const medications = ref<Medication[]>([])
const alertActive = ref(false)
const interactionPair = ref<[string, string] | null>(null)

function parseEventsText(eventsText: string): Array<Record<string, string>> {
  const events: Array<Record<string, string>> = []
  for (const line of eventsText.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('BATCH')) continue
    // Parse: EventType { key: "val", key2: val }
    const match = trimmed.match(/^(\w+)\s*\{(.*)\}$/)
    if (!match) continue
    const eventType = match[1]
    const fieldsStr = match[2]
    const fields: Record<string, string> = { event_type: eventType }
    // Parse key: value pairs
    const fieldRegex = /(\w+):\s*(".*?"|[\d.]+|\w+)/g
    let fm
    while ((fm = fieldRegex.exec(fieldsStr)) !== null) {
      fields[fm[1]] = fm[2].replace(/^"|"$/g, '')
    }
    events.push(fields)
  }
  return events
}

function onStepChanged(index: number) {
  const step = patientSafetyScenario.steps[index]
  if (!step) return

  const events = parseEventsText(step.eventsText)
  for (const evt of events) {
    if (evt.event_type === 'VitalSign') {
      const hr = parseFloat(evt.heart_rate)
      const spo2 = parseFloat(evt.spo2)
      if (!isNaN(hr) && !isNaN(spo2)) {
        readings.value.push({ heartRate: hr, spo2 })
      }
    } else if (evt.event_type === 'Prescription') {
      medications.value.push({
        time: new Date().toLocaleTimeString(),
        drug: evt.drug_name || 'Unknown',
        dosage: `${evt.dosage_mg || '?'}mg`,
        doctor: evt.doctor_id || 'Unknown',
        status: 'safe',
      })
    }
  }
}

function onAlerts(alertList: Array<Record<string, unknown>>) {
  for (const alert of alertList) {
    const alertType = String(alert.alert_type || alert.event_type || '')

    if (alertType === 'drug_interaction') {
      const drug1 = String(alert.drug1 || '')
      const drug2 = String(alert.drug2 || '')
      interactionPair.value = [drug1, drug2]
      // Update medication statuses for the interacting drugs
      for (const med of medications.value) {
        if (med.drug === drug1 || med.drug === drug2) {
          med.status = 'danger'
        }
      }
    } else if (alertType === 'vital_deterioration') {
      alertActive.value = true
    } else if (alertType === 'dosage_anomaly') {
      medications.value.push({
        time: new Date().toLocaleTimeString(),
        drug: String(alert.drug_name || 'Unknown'),
        dosage: `${alert.prescribed_mg || '?'}mg (max: ${alert.max_safe_mg || '?'}mg)`,
        doctor: 'System Alert',
        status: 'danger',
      })
    }
  }
}
</script>

<template>
  <DemoShell
    :scenario="patientSafetyScenario"
    @alerts="onAlerts"
    @step-changed="onStepChanged"
  >
    <template #hero>
      <v-row class="fill-height ma-0">
        <!-- Left column: Vitals Monitor -->
        <v-col cols="12" md="7" class="d-flex flex-column">
          <VitalsMonitor :readings="readings" :alert-active="alertActive" />
        </v-col>

        <!-- Right column: Patient info + Medication timeline -->
        <v-col cols="12" md="5" class="d-flex flex-column">
          <!-- Patient Info Card -->
          <v-card
            variant="flat"
            class="mb-4"
            color="rgba(255,255,255,0.05)"
            style="border: 1px solid rgba(255,255,255,0.08); border-radius: 12px"
          >
            <v-card-text class="pa-4">
              <div class="d-flex align-center mb-3">
                <v-avatar color="teal" size="40" class="mr-3">
                  <v-icon>mdi-account</v-icon>
                </v-avatar>
                <div>
                  <div class="text-white font-weight-bold">Patient Monitor</div>
                  <div class="text-caption text-medium-emphasis">Real-time CEP</div>
                </div>
              </div>
              <v-divider class="mb-3 border-opacity-10" />
              <div class="d-flex justify-space-between mb-1">
                <span class="text-medium-emphasis text-caption">Patient ID</span>
                <span class="text-white text-caption font-weight-medium">P-101 / P-202 / P-303</span>
              </div>
              <div class="d-flex justify-space-between mb-1">
                <span class="text-medium-emphasis text-caption">Ward</span>
                <span class="text-white text-caption font-weight-medium">ICU-3 East</span>
              </div>
              <div class="d-flex justify-space-between mb-1">
                <span class="text-medium-emphasis text-caption">Status</span>
                <v-chip
                  :color="alertActive ? 'red' : 'green'"
                  size="x-small"
                  label
                >
                  {{ alertActive ? 'ALERT' : 'Stable' }}
                </v-chip>
              </div>
              <div class="d-flex justify-space-between">
                <span class="text-medium-emphasis text-caption">Readings</span>
                <span class="text-white text-caption font-weight-medium">{{ readings.length }}</span>
              </div>
            </v-card-text>
          </v-card>

          <!-- Medication Timeline -->
          <MedicationTimeline
            :medications="medications"
            :interaction-pair="interactionPair"
          />
        </v-col>
      </v-row>
    </template>
  </DemoShell>
</template>
