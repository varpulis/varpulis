<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import RuleMatrix from '@/components/demos/RuleMatrix.vue'
import { socScaleScenario, mitreRules } from '@/data/scenarios/soc-scale'

const triggeredRules = ref<Map<string, number>>(new Map())

function onStepChanged(_index: number) {
  triggeredRules.value = new Map()
}

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    const alertType = String(alert.alert_type || '')
    if (alertType) {
      const current = triggeredRules.value.get(alertType) || 0
      triggeredRules.value.set(alertType, current + 1)
      // Trigger reactivity
      triggeredRules.value = new Map(triggeredRules.value)
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
    :scenario="socScaleScenario"
    @alerts="onAlerts"
    @step-changed="onStepChanged"
  >
    <template #hero>
      <v-row class="h-100 ma-0">
        <v-col cols="12" md="8" class="pa-4">
          <RuleMatrix
            :rules="mitreRules"
            :triggered-rules="triggeredRules"
          />
        </v-col>

        <v-col cols="12" md="4" class="pa-4">
          <v-card
            color="rgba(0, 0, 0, 0.5)"
            variant="flat"
            class="h-100"
            style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08)"
          >
            <v-card-title class="text-white pb-1">
              <v-icon class="mr-2" color="indigo">mdi-chart-timeline-variant</v-icon>
              Hamlet Sharing
            </v-card-title>
            <v-card-text>
              <div class="text-body-2 text-medium-emphasis mb-4">
                Overlapping event sequences across rules are processed once, not per-rule.
                Processing time stays flat as rules scale.
              </div>

              <div class="sharing-stat mb-3">
                <div class="text-caption text-medium-emphasis">Active Rules</div>
                <div class="text-h4 font-weight-bold text-white">
                  {{ triggeredRules.size }} <span class="text-body-2 text-medium-emphasis">/ {{ mitreRules.length }}</span>
                </div>
              </div>

              <div class="sharing-stat mb-3">
                <div class="text-caption text-medium-emphasis">Total Detections</div>
                <div class="text-h4 font-weight-bold text-error">
                  {{ Array.from(triggeredRules.values()).reduce((a, b) => a + b, 0) }}
                </div>
              </div>

              <div
                v-if="triggeredRules.size > 0"
                class="pa-3 rounded mt-4"
                style="background: rgba(92, 107, 192, 0.08); border: 1px solid rgba(92, 107, 192, 0.2)"
              >
                <div class="d-flex align-center">
                  <v-icon size="small" color="indigo" class="mr-2">mdi-information</v-icon>
                  <span class="text-caption" style="color: rgba(92, 107, 192, 0.9)">
                    {{ triggeredRules.size }} rules sharing event sequences via Hamlet optimization
                  </span>
                </div>
              </div>
            </v-card-text>
          </v-card>
        </v-col>
      </v-row>
    </template>

    <template #stats="{ eventsSent, matchCount, processingTimeMs }">
      <div v-if="eventsSent > 0" class="soc-stats d-flex align-center px-4 py-2 ga-4">
        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="indigo-lighten-2" class="mr-2">mdi-shield-lock</v-icon>
          <span class="stat-label">Rules Active</span>
          <span class="stat-value ml-2">{{ triggeredRules.size }} / {{ mitreRules.length }}</span>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="error" class="mr-2">mdi-alert-circle</v-icon>
          <span class="stat-label">Detections</span>
          <span class="stat-value ml-2 text-error">{{ matchCount }}</span>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="blue-lighten-2" class="mr-2">mdi-arrow-up-bold</v-icon>
          <span class="stat-label">Events</span>
          <span class="stat-value ml-2">{{ eventsSent }}</span>
        </div>

        <v-divider vertical class="mx-1 divider" />

        <div class="stat-box d-flex align-center">
          <v-icon size="small" color="green-lighten-2" class="mr-2">mdi-timer-outline</v-icon>
          <span class="stat-label">Processing</span>
          <span class="stat-value ml-2 text-success">{{ formattedTime(processingTimeMs) }}</span>
        </div>

        <v-spacer />

        <v-chip
          v-if="triggeredRules.size >= 5"
          color="indigo"
          size="small"
          variant="elevated"
          class="font-weight-bold"
        >
          Hamlet shared processing
        </v-chip>
      </div>
    </template>
  </DemoShell>
</template>

<style scoped>
.soc-stats {
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

.sharing-stat {
  padding: 12px;
  background: rgba(255, 255, 255, 0.03);
  border-radius: 8px;
  border: 1px solid rgba(255, 255, 255, 0.06);
}
</style>
