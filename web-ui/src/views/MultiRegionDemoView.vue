<script lang="ts">
export default { name: 'MultiRegionDemoView' }
</script>

<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import { multiRegionScenario } from '@/data/scenarios/multi-region'

const alertCount = ref(0)
const detectedAlerts = ref<
  Array<{
    type: string
    account: string
    txRegion: string
    cbRegion: string
    amount: number
    fields: Record<string, unknown>
    timestamp: number
  }>
>([])

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    alertCount.value++
    detectedAlerts.value.unshift({
      type: String(alert.alert_type || 'cross_region_fraud'),
      account: String(alert.account || 'unknown'),
      txRegion: String(alert.transaction_region || '?'),
      cbRegion: String(alert.chargeback_region || '?'),
      amount: Number(alert.amount || 0),
      fields: alert,
      timestamp: Date.now(),
    })
  }
}

function regionColor(region: string): string {
  if (region.includes('us')) return '#42A5F5'
  if (region.includes('eu')) return '#66BB6A'
  if (region.includes('ap')) return '#FFC107'
  return '#9E9E9E'
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleTimeString()
}
</script>

<template>
  <DemoShell :scenario="multiRegionScenario" @alerts="onAlerts">
    <template #hero>
      <v-card
        color="rgba(0, 0, 0, 0.5)"
        variant="flat"
        class="h-100"
        style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08)"
      >
        <v-card-title class="d-flex align-center text-white">
          <v-icon class="mr-2" color="indigo">mdi-earth</v-icon>
          Cross-Region Alerts
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
                <v-icon color="error" size="28">mdi-alert-decagram</v-icon>
              </template>

              <v-list-item-title class="text-white font-weight-medium text-body-2">
                {{ alert.account }} — ${{ alert.amount.toLocaleString() }}
              </v-list-item-title>

              <v-list-item-subtitle class="text-medium-emphasis">
                <span class="text-caption">{{ formatTimestamp(alert.timestamp) }}</span>
                <span class="mx-1">|</span>
                <v-chip
                  :color="regionColor(alert.txRegion)"
                  size="x-small"
                  variant="tonal"
                  class="mr-1"
                >
                  {{ alert.txRegion }}
                </v-chip>
                <v-icon size="12" class="mx-1" color="grey">mdi-arrow-right</v-icon>
                <v-chip :color="regionColor(alert.cbRegion)" size="x-small" variant="tonal">
                  {{ alert.cbRegion }}
                </v-chip>
              </v-list-item-subtitle>

              <template #append>
                <v-chip color="error" size="x-small" variant="tonal" label>
                  cross-region
                </v-chip>
              </template>
            </v-list-item>
          </v-list>

          <div
            v-else
            class="d-flex flex-column align-center justify-center text-medium-emphasis pa-8"
          >
            <v-icon size="48" class="mb-3" color="grey-darken-1">mdi-earth</v-icon>
            <div class="text-body-2">No cross-region alerts</div>
            <div class="text-caption mt-1">Inject events to trigger federation detection</div>
          </div>
        </v-card-text>
      </v-card>
    </template>
  </DemoShell>
</template>
