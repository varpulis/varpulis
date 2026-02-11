<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import ComparisonPanel from '@/components/demos/ComparisonPanel.vue'
import { blindSpotScenario } from '@/data/scenarios/blind-spot'

// Per-step greedy expected counts:
// Step 0 "Clear Signal": greedy finds 1
// Step 1 "The Blind Spot": greedy finds 0 (blocked by noise)
// Step 2 "Buried in Traffic": greedy finds 1 (only the unblocked chain)
// Step 3 "Clean Traffic": greedy finds 0 (no attacks)
const greedyExpected = [1, 0, 1, 0]

const currentStepIndex = ref(0)
const varpulisCount = ref(0)
const greedyCount = ref(greedyExpected[0])
const varpulisDetected = ref<string[]>([])
const greedyMissed = ref<string[]>([])
const hasInjected = ref(false)

function onStepChanged(index: number) {
  currentStepIndex.value = index
  // Reset comparison state for new step
  varpulisCount.value = 0
  greedyCount.value = greedyExpected[index] ?? 0
  varpulisDetected.value = []
  greedyMissed.value = []
  hasInjected.value = false
}

function onInjected() {
  hasInjected.value = true
}

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    varpulisCount.value++
    const target = String(alert.target || 'unknown')
    const attacker = String(alert.attacker || 'unknown')
    const method = String(alert.method || 'unknown')
    const label = `${attacker} → ${target} (${method})`
    varpulisDetected.value.push(label)
  }

  // Compute missed: chains Varpulis found beyond greedy's count
  const expected = greedyExpected[currentStepIndex.value] ?? 0
  if (varpulisCount.value > expected) {
    greedyMissed.value = varpulisDetected.value.slice(expected)
  }
}

function formattedTime(ms: number): string {
  if (ms < 1) return '<1ms'
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.round(ms)}ms`
}
</script>

<template>
  <DemoShell
    :scenario="blindSpotScenario"
    @alerts="onAlerts"
    @step-changed="onStepChanged"
    @injected="onInjected"
  >
    <template #hero>
      <ComparisonPanel
        :greedy-count="greedyCount"
        :varpulis-count="varpulisCount"
        :greedy-missed="greedyMissed"
        :varpulis-detected="varpulisDetected"
        :has-injected="hasInjected"
      />
    </template>

    <template #stats="{ eventsSent, processingTimeMs }">
      <div v-if="eventsSent > 0" class="blind-spot-stats d-flex align-center px-4 py-2 ga-4">
        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="blue-lighten-2" class="mr-2">mdi-arrow-up-bold</v-icon>
          <span class="stat-label">Events</span>
          <span class="stat-value ml-2">{{ eventsSent }}</span>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="orange" class="mr-2">mdi-fire</v-icon>
          <span class="stat-label">Greedy</span>
          <span class="stat-value ml-2">{{ greedyCount }} detected</span>
          <v-chip
            v-if="greedyMissed.length > 0"
            color="error"
            size="x-small"
            variant="flat"
            class="ml-2"
          >
            {{ greedyMissed.length }} missed
          </v-chip>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="primary" class="mr-2">mdi-shield-check</v-icon>
          <span class="stat-label">Varpulis</span>
          <span class="stat-value ml-2 text-primary">{{ varpulisCount }} detected</span>
          <v-chip
            color="success"
            size="x-small"
            variant="flat"
            class="ml-2"
          >
            0 missed
          </v-chip>
        </div>

        <v-spacer />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="green-lighten-2" class="mr-2">mdi-timer-outline</v-icon>
          <span class="stat-value">{{ formattedTime(processingTimeMs) }}</span>
        </div>
      </div>
    </template>
  </DemoShell>
</template>

<style scoped>
.blind-spot-stats {
  background: rgba(0, 0, 0, 0.4);
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  min-height: 40px;
  flex-wrap: wrap;
}

.stat-label {
  font-size: 12px;
  color: rgba(255, 255, 255, 0.5);
  white-space: nowrap;
}

.stat-value {
  font-size: 14px;
  font-weight: 700;
  color: rgba(255, 255, 255, 0.9);
  font-variant-numeric: tabular-nums;
}

.divider {
  opacity: 0.15;
}
</style>
