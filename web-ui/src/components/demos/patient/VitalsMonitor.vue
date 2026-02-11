<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EChartsOption } from 'echarts'

const props = defineProps<{
  readings: Array<{ heartRate: number; spo2: number }>
  alertActive: boolean
}>()

const currentHeartRate = computed(() => {
  if (props.readings.length === 0) return '--'
  return props.readings[props.readings.length - 1].heartRate
})

const currentSpO2 = computed(() => {
  if (props.readings.length === 0) return '--'
  return props.readings[props.readings.length - 1].spo2
})

const hrLineColor = computed(() => (props.alertActive ? '#FF5252' : '#4CAF50'))
const spo2LineColor = computed(() => (props.alertActive ? '#FF5252' : '#00BFA5'))

const heartRateOption = computed<EChartsOption>(() => {
  const data = props.readings.map((r, i) => [i, r.heartRate])
  return {
    grid: {
      left: 50,
      right: 90,
      top: 20,
      bottom: 20,
    },
    xAxis: {
      type: 'value',
      show: false,
      min: 0,
      max: Math.max(data.length - 1, 20),
    },
    yAxis: {
      type: 'value',
      min: 60,
      max: 180,
      name: 'BPM',
      nameTextStyle: {
        color: '#4CAF50',
        fontSize: 10,
      },
      axisLabel: {
        color: '#4CAF50',
        fontSize: 10,
      },
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: {
        lineStyle: {
          color: 'rgba(76, 175, 80, 0.1)',
        },
      },
    },
    series: [
      {
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: {
          color: hrLineColor.value,
          width: 2,
        },
        data,
      },
    ],
    animation: false,
  }
})

const spo2Option = computed<EChartsOption>(() => {
  const data = props.readings.map((r, i) => [i, r.spo2])
  return {
    grid: {
      left: 50,
      right: 90,
      top: 20,
      bottom: 20,
    },
    xAxis: {
      type: 'value',
      show: false,
      min: 0,
      max: Math.max(data.length - 1, 20),
    },
    yAxis: {
      type: 'value',
      min: 80,
      max: 100,
      name: 'SpO2 %',
      nameTextStyle: {
        color: '#00BFA5',
        fontSize: 10,
      },
      axisLabel: {
        color: '#00BFA5',
        fontSize: 10,
      },
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: {
        lineStyle: {
          color: 'rgba(0, 191, 165, 0.1)',
        },
      },
    },
    series: [
      {
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: {
          color: spo2LineColor.value,
          width: 2,
        },
        data,
      },
    ],
    animation: false,
  }
})
</script>

<template>
  <v-card class="vitals-card" variant="flat">
    <!-- Heart Rate -->
    <div class="vitals-row">
      <div class="chart-area">
        <v-chart
          v-if="readings.length > 0"
          :option="heartRateOption"
          style="height: 150px; width: 100%"
          autoresize
        />
        <div
          v-else
          class="d-flex align-center justify-center"
          style="height: 150px"
        >
          <span class="text-grey">Awaiting heart rate data...</span>
        </div>
      </div>
      <div class="current-value" :class="{ 'value-flash': alertActive }">
        <div class="value-number" :style="{ color: alertActive ? '#FF5252' : '#4CAF50' }">
          {{ currentHeartRate }}
        </div>
        <div class="value-label" :style="{ color: alertActive ? '#FF5252' : '#4CAF50' }">
          BPM
        </div>
      </div>
    </div>

    <v-divider class="border-opacity-10" />

    <!-- SpO2 -->
    <div class="vitals-row">
      <div class="chart-area">
        <v-chart
          v-if="readings.length > 0"
          :option="spo2Option"
          style="height: 150px; width: 100%"
          autoresize
        />
        <div
          v-else
          class="d-flex align-center justify-center"
          style="height: 150px"
        >
          <span class="text-grey">Awaiting SpO2 data...</span>
        </div>
      </div>
      <div class="current-value" :class="{ 'value-flash': alertActive }">
        <div class="value-number" :style="{ color: alertActive ? '#FF5252' : '#00BFA5' }">
          {{ currentSpO2 }}
        </div>
        <div class="value-label" :style="{ color: alertActive ? '#FF5252' : '#00BFA5' }">
          SpO2 %
        </div>
      </div>
    </div>
  </v-card>
</template>

<style scoped>
.vitals-card {
  background: #111;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  overflow: hidden;
}

.vitals-row {
  display: flex;
  align-items: center;
}

.chart-area {
  flex: 1;
  min-width: 0;
}

.current-value {
  width: 80px;
  text-align: center;
  padding-right: 12px;
  flex-shrink: 0;
}

.value-number {
  font-size: 32px;
  font-weight: 700;
  font-family: 'Roboto Mono', monospace;
  line-height: 1.1;
}

.value-label {
  font-size: 12px;
  font-weight: 500;
  text-transform: uppercase;
  opacity: 0.8;
}

@keyframes flash {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}

.value-flash .value-number {
  animation: flash 0.8s ease-in-out infinite;
}
</style>
