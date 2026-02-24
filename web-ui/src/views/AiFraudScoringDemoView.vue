<script lang="ts">
export default { name: 'AiFraudScoringDemoView' }
</script>

<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import { aiFraudScoringScenario } from '@/data/scenarios/ai-fraud-scoring'

const alertCount = ref(0)
const detectedAlerts = ref<
  Array<{
    type: string
    account: string
    amount: number
    probability: number
    fields: Record<string, unknown>
    timestamp: number
  }>
>([])

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    alertCount.value++
    detectedAlerts.value.unshift({
      type: String(alert.alert_type || 'ai_fraud_alert'),
      account: String(alert.account || 'unknown'),
      amount: Number(alert.amount || 0),
      probability: Number(alert.fraud_probability || 0),
      fields: alert,
      timestamp: Date.now(),
    })
  }
}

function riskColor(prob: number): string {
  if (prob > 0.9) return '#FF1744'
  if (prob > 0.7) return '#FF5252'
  return '#FFC107'
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleTimeString()
}
</script>

<template>
  <DemoShell :scenario="aiFraudScoringScenario" @alerts="onAlerts">
    <template #hero>
      <v-card
        color="rgba(0, 0, 0, 0.5)"
        variant="flat"
        class="h-100"
        style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08)"
      >
        <v-card-title class="d-flex align-center text-white">
          <v-icon class="mr-2" color="deep-orange">mdi-brain</v-icon>
          AI Fraud Alerts
          <v-spacer />
          <v-chip v-if="alertCount > 0" color="error" size="small" variant="flat">
            {{ alertCount }}
          </v-chip>
        </v-card-title>

        <v-card-text class="pa-0" style="overflow-y: auto; max-height: 460px">
          <v-list v-if="detectedAlerts.length > 0" bg-color="transparent" density="comfortable">
            <v-list-item
              v-for="(alert, index) in detectedAlerts"
              :key="`${alert.account}-${alert.timestamp}-${index}`"
              class="mb-1"
            >
              <template #prepend>
                <v-icon :color="riskColor(alert.probability)" size="28">
                  mdi-shield-alert
                </v-icon>
              </template>

              <v-list-item-title class="text-white font-weight-medium text-body-2">
                {{ alert.account }} — ${{ alert.amount.toLocaleString() }}
              </v-list-item-title>

              <v-list-item-subtitle class="text-medium-emphasis">
                <span class="text-caption">{{ formatTimestamp(alert.timestamp) }}</span>
                <span class="mx-1">|</span>
                <span class="text-caption">
                  Score:
                  <strong :style="{ color: riskColor(alert.probability) }">
                    {{ (alert.probability * 100).toFixed(1) }}%
                  </strong>
                </span>
              </v-list-item-subtitle>

              <template #append>
                <v-chip :color="riskColor(alert.probability)" size="x-small" variant="tonal" label>
                  {{ alert.probability > 0.9 ? 'Critical' : 'High' }}
                </v-chip>
              </template>
            </v-list-item>
          </v-list>

          <div
            v-else
            class="d-flex flex-column align-center justify-center text-medium-emphasis pa-8"
          >
            <v-icon size="48" class="mb-3" color="grey-darken-1">mdi-shield-check-outline</v-icon>
            <div class="text-body-2">No fraud alerts</div>
            <div class="text-caption mt-1">Inject transactions to trigger ML scoring</div>
          </div>
        </v-card-text>
      </v-card>
    </template>
  </DemoShell>
</template>
