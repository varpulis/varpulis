<script setup lang="ts">
import type { MitreRule } from '@/data/scenarios/soc-scale'

defineProps<{
  rules: MitreRule[]
  triggeredRules: Map<string, number>
}>()
</script>

<template>
  <v-card
    color="rgba(0, 0, 0, 0.5)"
    variant="flat"
    class="h-100"
    style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08)"
  >
    <v-card-title class="d-flex align-center text-white pb-1">
      <v-icon class="mr-2" color="indigo">mdi-shield-lock</v-icon>
      Detection Rules
      <v-spacer />
      <v-chip
        :color="triggeredRules.size > 0 ? 'error' : 'grey-darken-1'"
        size="small"
        variant="flat"
      >
        {{ triggeredRules.size }} / {{ rules.length }} active
      </v-chip>
    </v-card-title>

    <v-card-text class="pa-2">
      <div
        v-for="rule in rules"
        :key="rule.alertType"
        class="rule-row d-flex align-center px-3 py-2 mb-1 rounded"
        :class="{ 'rule-triggered': triggeredRules.has(rule.alertType) }"
      >
        <v-icon
          size="18"
          :color="triggeredRules.has(rule.alertType) ? 'error' : 'grey-darken-1'"
          class="mr-3"
        >
          {{ triggeredRules.has(rule.alertType) ? 'mdi-alert-circle' : 'mdi-circle-outline' }}
        </v-icon>

        <div class="flex-grow-1">
          <div class="d-flex align-center">
            <span
              class="text-body-2 font-weight-medium"
              :style="{ color: triggeredRules.has(rule.alertType) ? '#FF5252' : 'rgba(255,255,255,0.7)' }"
            >
              {{ rule.name }}
            </span>
            <v-chip
              size="x-small"
              variant="outlined"
              :color="triggeredRules.has(rule.alertType) ? 'error' : 'grey'"
              class="ml-2"
              label
            >
              {{ rule.mitreId }}
            </v-chip>
          </div>
        </div>

        <v-chip
          v-if="triggeredRules.has(rule.alertType)"
          color="error"
          size="x-small"
          variant="flat"
          class="ml-2"
        >
          {{ triggeredRules.get(rule.alertType) }}
        </v-chip>

        <div
          v-else
          class="text-caption text-medium-emphasis ml-2"
          style="min-width: 40px; text-align: right"
        >
          idle
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<style scoped>
.rule-row {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.04);
  transition: all 0.3s ease;
}

.rule-triggered {
  background: rgba(255, 82, 82, 0.08);
  border-color: rgba(255, 82, 82, 0.2);
  animation: rule-flash 0.5s ease-out;
}

@keyframes rule-flash {
  0% { background: rgba(255, 82, 82, 0.3); }
  100% { background: rgba(255, 82, 82, 0.08); }
}
</style>
