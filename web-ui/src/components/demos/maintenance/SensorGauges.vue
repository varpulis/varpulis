<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EChartsOption } from 'echarts'

const props = withDefaults(
  defineProps<{
    vibration?: number
    temperature?: number
  }>(),
  {
    vibration: 0,
    temperature: 0,
  }
)

const vibrationOption = computed<EChartsOption>(() => ({
  series: [
    {
      type: 'gauge',
      min: 0,
      max: 1.5,
      splitNumber: 6,
      animationDuration: 1000,
      axisLine: {
        lineStyle: {
          width: 14,
          color: [
            [0.5 / 1.5, '#4CAF50'],
            [0.8 / 1.5, '#FFC107'],
            [1, '#FF5252'],
          ],
        },
      },
      axisTick: {
        length: 6,
        lineStyle: { color: 'auto' },
      },
      splitLine: {
        length: 14,
        lineStyle: { color: 'auto', width: 2 },
      },
      axisLabel: {
        distance: 20,
        color: '#9E9E9E',
        fontSize: 11,
      },
      pointer: {
        width: 5,
        length: '60%',
        itemStyle: { color: 'auto' },
      },
      anchor: {
        show: true,
        size: 12,
        itemStyle: {
          borderWidth: 2,
          borderColor: '#9E9E9E',
        },
      },
      title: {
        offsetCenter: [0, '70%'],
        color: '#9E9E9E',
        fontSize: 14,
      },
      detail: {
        valueAnimation: true,
        formatter: '{value} mm',
        offsetCenter: [0, '90%'],
        color: '#FFFFFF',
        fontSize: 20,
        fontWeight: 'bold',
      },
      data: [
        {
          value: props.vibration,
          name: 'Vibration',
        },
      ],
    },
  ],
}))

const temperatureOption = computed<EChartsOption>(() => ({
  series: [
    {
      type: 'gauge',
      min: 0,
      max: 100,
      splitNumber: 10,
      animationDuration: 1000,
      axisLine: {
        lineStyle: {
          width: 14,
          color: [
            [0.5, '#4CAF50'],
            [0.6, '#FFC107'],
            [1, '#FF5252'],
          ],
        },
      },
      axisTick: {
        length: 6,
        lineStyle: { color: 'auto' },
      },
      splitLine: {
        length: 14,
        lineStyle: { color: 'auto', width: 2 },
      },
      axisLabel: {
        distance: 20,
        color: '#9E9E9E',
        fontSize: 11,
      },
      pointer: {
        width: 5,
        length: '60%',
        itemStyle: { color: 'auto' },
      },
      anchor: {
        show: true,
        size: 12,
        itemStyle: {
          borderWidth: 2,
          borderColor: '#9E9E9E',
        },
      },
      title: {
        offsetCenter: [0, '70%'],
        color: '#9E9E9E',
        fontSize: 14,
      },
      detail: {
        valueAnimation: true,
        formatter: '{value} \u00B0C',
        offsetCenter: [0, '90%'],
        color: '#FFFFFF',
        fontSize: 20,
        fontWeight: 'bold',
      },
      data: [
        {
          value: props.temperature,
          name: 'Temperature',
        },
      ],
    },
  ],
}))
</script>

<template>
  <div class="sensor-gauges d-flex">
    <div class="gauge-container">
      <v-chart :option="vibrationOption" autoresize style="height: 250px" />
    </div>
    <div class="gauge-container">
      <v-chart :option="temperatureOption" autoresize style="height: 250px" />
    </div>
  </div>
</template>

<style scoped>
.sensor-gauges {
  gap: 8px;
  width: 100%;
}

.gauge-container {
  flex: 1;
  min-width: 0;
  background: rgba(0, 0, 0, 0.3);
  border-radius: 8px;
  padding: 4px;
}
</style>
