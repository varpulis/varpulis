<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import SensorGauges from '@/components/demos/maintenance/SensorGauges.vue'
import TrendChart from '@/components/demos/maintenance/TrendChart.vue'
import { predictiveMaintenanceScenario } from '@/data/scenarios/predictive-maintenance'

const currentVibration = ref(0)
const currentTemperature = ref(0)
const machineId = ref('--')
const machineStatus = ref<'normal' | 'warning' | 'critical'>('normal')
const readings = ref<Array<{ time: number; vibration: number; temperature: number }>>([])

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    const now = Date.now()

    // Handle bearing degradation alerts
    if (alert.alert_type === 'bearing_degradation') {
      const vibration = Number(alert.current_amplitude ?? alert.initial_amplitude ?? 0)
      const initialVibration = Number(alert.initial_amplitude ?? 0)
      currentVibration.value = vibration
      machineId.value = String(alert.machine_id ?? '--')
      machineStatus.value = vibration >= 0.8 ? 'critical' : vibration >= 0.5 ? 'warning' : 'normal'

      // Push the initial reading if this is the first data for this machine
      if (readings.value.length === 0 || readings.value[readings.value.length - 1].vibration !== initialVibration) {
        readings.value.push({
          time: now - 1000,
          vibration: initialVibration,
          temperature: currentTemperature.value,
        })
      }
      readings.value.push({
        time: now,
        vibration,
        temperature: currentTemperature.value,
      })
    }

    // Handle overheating alerts
    if (alert.alert_type === 'overheating') {
      const temperature = Number(alert.spike_temp ?? alert.baseline_temp ?? 0)
      const baselineTemp = Number(alert.baseline_temp ?? 0)
      currentTemperature.value = temperature
      if (alert.zone_id) {
        machineId.value = String(alert.zone_id)
      }
      machineStatus.value = temperature >= 60 ? 'critical' : temperature >= 50 ? 'warning' : 'normal'

      // Push the baseline reading if needed
      if (readings.value.length === 0 || readings.value[readings.value.length - 1].temperature !== baselineTemp) {
        readings.value.push({
          time: now - 1000,
          vibration: currentVibration.value,
          temperature: baselineTemp,
        })
      }
      readings.value.push({
        time: now,
        vibration: currentVibration.value,
        temperature,
      })
    }

    // Handle raw sensor events that appear in output_events
    if (alert.event_type === 'VibrationReading' || alert.event_type === 'TemperatureReading') {
      if (alert.amplitude !== undefined) {
        const v = Number(alert.amplitude)
        currentVibration.value = v
        if (alert.machine_id) machineId.value = String(alert.machine_id)
        readings.value.push({
          time: now,
          vibration: v,
          temperature: currentTemperature.value,
        })
      }
      if (alert.temp_c !== undefined) {
        const t = Number(alert.temp_c)
        currentTemperature.value = t
        if (alert.zone_id) machineId.value = String(alert.zone_id)
        readings.value.push({
          time: now,
          vibration: currentVibration.value,
          temperature: t,
        })
      }
    }
  }
}

const statusColor: Record<string, string> = {
  normal: '#4CAF50',
  warning: '#FFC107',
  critical: '#FF5252',
}

const statusIcon: Record<string, string> = {
  normal: 'mdi-check-circle',
  warning: 'mdi-alert',
  critical: 'mdi-alert-octagon',
}

const statusLabel: Record<string, string> = {
  normal: 'Healthy',
  warning: 'Degrading',
  critical: 'Critical',
}
</script>

<template>
  <DemoShell :scenario="predictiveMaintenanceScenario" @alerts="onAlerts">
    <template #hero>
      <div class="maintenance-layout">
        <!-- Top row: Gauges + Machine Status -->
        <div class="top-row d-flex">
          <div class="gauges-panel flex-grow-1">
            <SensorGauges
              :vibration="currentVibration"
              :temperature="currentTemperature"
            />
          </div>

          <v-card
            class="machine-status-card ml-4"
            color="rgba(0, 0, 0, 0.4)"
            variant="outlined"
          >
            <v-card-title class="text-body-1 font-weight-bold text-white pb-1">
              <v-icon class="mr-2" size="20">mdi-cog-sync</v-icon>
              Machine Status
            </v-card-title>
            <v-card-text class="pt-2">
              <div class="d-flex flex-column align-center">
                <v-icon
                  :color="statusColor[machineStatus]"
                  size="56"
                  class="mb-3"
                >
                  {{ statusIcon[machineStatus] }}
                </v-icon>

                <div
                  class="text-h5 font-weight-bold mb-2"
                  :style="{ color: statusColor[machineStatus] }"
                >
                  {{ statusLabel[machineStatus] }}
                </div>

                <v-divider class="my-2 w-100" />

                <div class="d-flex align-center mt-1">
                  <v-icon size="16" class="mr-1 text-medium-emphasis">mdi-identifier</v-icon>
                  <span class="text-body-2 text-medium-emphasis">Machine:</span>
                  <span class="text-body-2 text-white ml-1 font-weight-medium">{{ machineId }}</span>
                </div>

                <div class="d-flex align-center mt-1">
                  <v-icon size="16" class="mr-1 text-medium-emphasis">mdi-vibrate</v-icon>
                  <span class="text-body-2 text-medium-emphasis">Vibration:</span>
                  <span class="text-body-2 text-white ml-1 font-weight-medium">
                    {{ currentVibration.toFixed(2) }} mm
                  </span>
                </div>

                <div class="d-flex align-center mt-1">
                  <v-icon size="16" class="mr-1 text-medium-emphasis">mdi-thermometer</v-icon>
                  <span class="text-body-2 text-medium-emphasis">Temperature:</span>
                  <span class="text-body-2 text-white ml-1 font-weight-medium">
                    {{ currentTemperature.toFixed(1) }} &deg;C
                  </span>
                </div>

                <div class="d-flex align-center mt-1">
                  <v-icon size="16" class="mr-1 text-medium-emphasis">mdi-counter</v-icon>
                  <span class="text-body-2 text-medium-emphasis">Readings:</span>
                  <span class="text-body-2 text-white ml-1 font-weight-medium">
                    {{ readings.length }}
                  </span>
                </div>
              </div>
            </v-card-text>
          </v-card>
        </div>

        <!-- Bottom row: Trend chart -->
        <div class="bottom-row mt-4">
          <TrendChart :readings="readings" />
        </div>
      </div>
    </template>
  </DemoShell>
</template>

<style scoped>
.maintenance-layout {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.top-row {
  flex: 0 0 40%;
  min-height: 0;
}

.gauges-panel {
  min-width: 0;
}

.machine-status-card {
  flex: 0 0 240px;
  border-color: rgba(255, 255, 255, 0.12) !important;
}

.bottom-row {
  flex: 0 0 60%;
  min-height: 0;
}
</style>
