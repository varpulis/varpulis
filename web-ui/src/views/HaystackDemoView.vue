<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import HostGrid from '@/components/demos/HostGrid.vue'
import { haystackScenario, haystackHosts } from '@/data/scenarios/haystack'

const hostAlerts = ref<Map<string, number>>(new Map())

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    const target = String(alert.target || '')
    if (target) {
      const current = hostAlerts.value.get(target) || 0
      hostAlerts.value.set(target, current + 1)
      // Trigger reactivity
      hostAlerts.value = new Map(hostAlerts.value)
    }
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
    :scenario="haystackScenario"
    @alerts="onAlerts"
  >
    <template #hero>
      <div class="pa-4">
        <div class="text-h6 text-white mb-4 d-flex align-center">
          <v-icon class="mr-2" color="teal">mdi-server-network</v-icon>
          Network Hosts ({{ haystackHosts.length }})
          <v-chip
            v-if="hostAlerts.size > 0"
            color="error"
            size="small"
            variant="flat"
            class="ml-3"
          >
            {{ hostAlerts.size }} compromised
          </v-chip>
        </div>
        <HostGrid
          :hosts="haystackHosts"
          :alerts="hostAlerts"
        />
      </div>
    </template>

    <template #stats="{ eventsSent, matchCount, processingTimeMs }">
      <div v-if="eventsSent > 0" class="haystack-stats d-flex align-center px-4 py-2 ga-4">
        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="blue-lighten-2" class="mr-2">mdi-arrow-up-bold</v-icon>
          <span class="stat-label">Events Processed</span>
          <span class="stat-value ml-2">{{ eventsSent }}</span>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="error" class="mr-2">mdi-target</v-icon>
          <span class="stat-label">Attacks Found</span>
          <span class="stat-value ml-2 text-error">{{ matchCount }}</span>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="green-lighten-2" class="mr-2">mdi-timer-outline</v-icon>
          <span class="stat-label">Processing Time</span>
          <span class="stat-value ml-2 text-success">{{ formattedTime(processingTimeMs) }}</span>
        </div>

        <v-spacer />

        <v-chip
          v-if="matchCount > 0"
          color="teal"
          size="small"
          variant="elevated"
          class="font-weight-bold"
        >
          {{ matchCount }} needles in {{ eventsSent }} events
        </v-chip>
      </div>
    </template>
  </DemoShell>
</template>

<style scoped>
.haystack-stats {
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
