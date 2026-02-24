<script lang="ts">
export default { name: 'IotConcurrentDemoView' }
</script>

<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import { iotConcurrentScenario } from '@/data/scenarios/iot-concurrent'

const alertCount = ref(0)
const detectedAlerts = ref<
  Array<{
    type: string
    sensor: string
    zone: string
    fields: Record<string, unknown>
    timestamp: number
  }>
>([])

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    alertCount.value++
    detectedAlerts.value.unshift({
      type: String(alert.alert_type || 'sensor_anomaly'),
      sensor: String(alert.sensor_id || 'unknown'),
      zone: String(alert.zone || 'unknown'),
      fields: alert,
      timestamp: Date.now(),
    })
  }
}

function zoneColor(zone: string): string {
  switch (zone) {
    case 'assembly':
      return '#42A5F5'
    case 'welding':
      return '#FF5252'
    case 'paint':
      return '#FFC107'
    case 'storage':
      return '#66BB6A'
    default:
      return '#9E9E9E'
  }
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleTimeString()
}
</script>

<template>
  <DemoShell :scenario="iotConcurrentScenario" @alerts="onAlerts">
    <template #hero>
      <v-card
        color="rgba(0, 0, 0, 0.5)"
        variant="flat"
        class="h-100"
        style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08)"
      >
        <v-card-title class="d-flex align-center text-white">
          <v-icon class="mr-2" color="teal">mdi-chip</v-icon>
          Sensor Alerts
          <v-spacer />
          <v-chip v-if="alertCount > 0" color="error" size="small" variant="flat">
            {{ alertCount }}
          </v-chip>
        </v-card-title>

        <v-card-text class="pa-0" style="overflow-y: auto; max-height: 460px">
          <v-list v-if="detectedAlerts.length > 0" bg-color="transparent" density="comfortable">
            <v-list-item
              v-for="(alert, index) in detectedAlerts"
              :key="`${alert.sensor}-${alert.timestamp}-${index}`"
              class="mb-1"
            >
              <template #prepend>
                <v-icon :color="zoneColor(alert.zone)" size="28">mdi-thermometer-alert</v-icon>
              </template>

              <v-list-item-title class="text-white font-weight-medium text-body-2">
                {{ alert.zone }} zone — {{ alert.sensor }}
              </v-list-item-title>

              <v-list-item-subtitle class="text-medium-emphasis">
                <span class="text-caption">{{ formatTimestamp(alert.timestamp) }}</span>
                <template v-if="alert.fields.temperature">
                  <span class="mx-1">|</span>
                  <span class="text-caption">Temp: {{ alert.fields.temperature }}C</span>
                </template>
                <template v-if="alert.fields.humidity">
                  <span class="mx-1">|</span>
                  <span class="text-caption">Humidity: {{ alert.fields.humidity }}%</span>
                </template>
              </v-list-item-subtitle>

              <template #append>
                <v-chip :color="zoneColor(alert.zone)" size="x-small" variant="tonal" label>
                  {{ alert.zone }}
                </v-chip>
              </template>
            </v-list-item>
          </v-list>

          <div
            v-else
            class="d-flex flex-column align-center justify-center text-medium-emphasis pa-8"
          >
            <v-icon size="48" class="mb-3" color="grey-darken-1">mdi-thermometer-check</v-icon>
            <div class="text-body-2">All sensors normal</div>
            <div class="text-caption mt-1">Inject events to trigger anomaly detection</div>
          </div>
        </v-card-text>
      </v-card>
    </template>
  </DemoShell>
</template>
