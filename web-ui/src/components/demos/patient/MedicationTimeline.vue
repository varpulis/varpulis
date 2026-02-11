<script setup lang="ts">
import { computed } from 'vue'

interface Medication {
  time: string
  drug: string
  dosage: string
  doctor: string
  status: 'safe' | 'danger' | 'warning'
}

const props = defineProps<{
  medications: Medication[]
  interactionPair: [string, string] | null
}>()

function dotColor(status: string): string {
  switch (status) {
    case 'danger':
      return 'red'
    case 'warning':
      return 'amber'
    default:
      return 'green'
  }
}

function dotIcon(status: string): string {
  switch (status) {
    case 'danger':
      return 'mdi-alert-circle'
    case 'warning':
      return 'mdi-alert'
    default:
      return 'mdi-check-circle'
  }
}

const interactionDrugs = computed(() => {
  if (!props.interactionPair) return new Set<string>()
  return new Set(props.interactionPair)
})

function isInteractionDrug(drug: string): boolean {
  return interactionDrugs.value.has(drug)
}

function shouldShowWarningAfter(index: number): boolean {
  if (!props.interactionPair) return false
  const current = props.medications[index]
  const next = props.medications[index + 1]
  if (!current || !next) return false
  return isInteractionDrug(current.drug) && isInteractionDrug(next.drug)
}
</script>

<template>
  <v-card variant="flat" color="transparent" class="medication-timeline">
    <v-card-title class="text-white text-body-1 font-weight-bold pb-2">
      <v-icon start size="small" color="teal">mdi-pill</v-icon>
      Medication Timeline
    </v-card-title>

    <v-card-text v-if="medications.length === 0" class="text-grey text-center py-6">
      No medications prescribed yet.
    </v-card-text>

    <v-card-text v-else class="pt-0">
      <v-timeline side="end" density="compact" truncate-line="both">
        <template v-for="(med, index) in medications" :key="index">
          <v-timeline-item
            :dot-color="dotColor(med.status)"
            :icon="dotIcon(med.status)"
            size="small"
          >
            <div
              class="med-entry pa-3 rounded"
              :class="{ 'interaction-highlight': isInteractionDrug(med.drug) }"
            >
              <div class="text-caption text-medium-emphasis">{{ med.time }}</div>
              <div class="text-white">
                <strong>{{ med.drug }}</strong>
                <span class="text-medium-emphasis ml-2">{{ med.dosage }}</span>
              </div>
              <div class="text-caption text-medium-emphasis">
                Prescribed by {{ med.doctor }}
              </div>
            </div>

            <!-- Interaction warning between the two drugs -->
            <div
              v-if="shouldShowWarningAfter(index)"
              class="interaction-warning d-flex align-center mt-3 mb-1 pa-2 rounded"
            >
              <v-icon color="red" size="small" class="mr-2">mdi-alert-octagon</v-icon>
              <span class="text-red text-caption font-weight-bold">
                Drug Interaction Detected
              </span>
            </div>
          </v-timeline-item>
        </template>
      </v-timeline>
    </v-card-text>
  </v-card>
</template>

<style scoped>
.medication-timeline {
  color: white;
}

.med-entry {
  background: rgba(255, 255, 255, 0.04);
  transition: all 0.3s ease;
}

.interaction-highlight {
  border: 1px solid #FF5252;
  background: rgba(255, 82, 82, 0.08);
}

.interaction-warning {
  background: rgba(255, 82, 82, 0.12);
  border: 1px dashed rgba(255, 82, 82, 0.5);
}
</style>
