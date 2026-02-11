<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  eventsSent: number
  matchCount: number
  traditionalCount: number
  processingTimeMs: number
}>()

const multiplier = computed(() => {
  if (props.traditionalCount === 0) return 0
  return Math.round(props.matchCount / props.traditionalCount)
})

const formattedTime = computed(() => {
  if (props.processingTimeMs < 1) return '<1ms'
  if (props.processingTimeMs >= 1000) return `${(props.processingTimeMs / 1000).toFixed(1)}s`
  return `${Math.round(props.processingTimeMs)}ms`
})
</script>

<template>
  <div class="stats-bar d-flex align-center px-4 py-2 ga-4">
    <div class="stat-box d-flex align-center">
      <v-icon size="small" color="blue-lighten-2" class="mr-2">mdi-arrow-up-bold</v-icon>
      <span class="stat-label">Events Injected</span>
      <span class="stat-value ml-2">{{ eventsSent }}</span>
    </div>

    <v-divider vertical class="mx-1 divider" />

    <div class="stat-box d-flex align-center">
      <v-icon size="small" color="amber" class="mr-2">mdi-target</v-icon>
      <span class="stat-label">Varpulis Matches</span>
      <span class="stat-value ml-2 text-amber">{{ matchCount }}</span>
    </div>

    <v-divider vertical class="mx-1 divider" />

    <div class="stat-box d-flex align-center">
      <v-icon size="small" color="grey" class="mr-2">mdi-target</v-icon>
      <span class="stat-label">Traditional CEP</span>
      <span class="stat-value ml-2">{{ traditionalCount }}</span>
    </div>

    <v-divider vertical class="mx-1 divider" />

    <v-chip
      v-if="multiplier > 1"
      color="success"
      size="small"
      variant="elevated"
      class="font-weight-bold"
    >
      {{ multiplier }}&times; more complete
    </v-chip>

    <v-spacer />

    <div class="stat-box d-flex align-center">
      <v-icon size="small" color="green-lighten-2" class="mr-2">mdi-timer-outline</v-icon>
      <span class="stat-value">{{ formattedTime }}</span>
    </div>
  </div>
</template>

<style scoped>
.stats-bar {
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
