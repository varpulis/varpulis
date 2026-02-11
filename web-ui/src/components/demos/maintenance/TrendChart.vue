<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EChartsOption } from 'echarts'

interface Reading {
  time: number
  vibration: number
  temperature: number
}

const props = withDefaults(
  defineProps<{
    readings?: Reading[]
  }>(),
  {
    readings: () => [],
  }
)

const VIBRATION_THRESHOLD = 0.8
const TEMPERATURE_THRESHOLD = 60

const chartOption = computed<EChartsOption>(() => {
  const data = props.readings

  // Find readings where values cross thresholds for markPoints
  const vibrationAlertPoints: Array<{ name: string; xAxis: number; yAxis: number }> = []
  const temperatureAlertPoints: Array<{ name: string; xAxis: number; yAxis: number }> = []

  for (let i = 0; i < data.length; i++) {
    if (data[i].vibration >= VIBRATION_THRESHOLD) {
      vibrationAlertPoints.push({ name: 'alert', xAxis: i, yAxis: data[i].vibration })
    }
    if (data[i].temperature >= TEMPERATURE_THRESHOLD) {
      temperatureAlertPoints.push({ name: 'alert', xAxis: i, yAxis: data[i].temperature })
    }
  }

  return {
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(30, 30, 30, 0.9)',
      borderColor: '#7C4DFF',
      textStyle: { color: '#fff' },
    },
    legend: {
      data: ['Vibration', 'Temperature'],
      textStyle: { color: '#9E9E9E' },
      top: 4,
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      top: '14%',
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      data: data.map((_, i) => i + 1),
      axisLine: { lineStyle: { color: '#546E7A' } },
      axisLabel: { color: '#9E9E9E' },
      splitLine: { show: false },
    },
    yAxis: [
      {
        type: 'value',
        name: 'Vibration (mm)',
        nameTextStyle: { color: '#7C4DFF' },
        axisLine: { show: true, lineStyle: { color: '#7C4DFF' } },
        axisLabel: { color: '#9E9E9E' },
        splitLine: {
          lineStyle: { color: 'rgba(84, 110, 122, 0.3)' },
        },
      },
      {
        type: 'value',
        name: 'Temp (\u00B0C)',
        nameTextStyle: { color: '#00BFA5' },
        axisLine: { show: true, lineStyle: { color: '#00BFA5' } },
        axisLabel: { color: '#9E9E9E' },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: 'Vibration',
        type: 'line',
        smooth: true,
        symbol: 'circle',
        symbolSize: 4,
        yAxisIndex: 0,
        lineStyle: { color: '#7C4DFF', width: 2 },
        itemStyle: { color: '#7C4DFF' },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(124, 77, 255, 0.4)' },
              { offset: 1, color: 'rgba(124, 77, 255, 0.05)' },
            ],
          },
        },
        data: data.map((r) => r.vibration),
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color: '#FF5252', type: 'dashed', width: 1 },
          data: [
            {
              yAxis: VIBRATION_THRESHOLD,
              label: {
                formatter: `${VIBRATION_THRESHOLD} mm`,
                color: '#FF5252',
                position: 'insideEndTop',
              },
            },
          ],
        },
        markPoint: {
          symbol: 'circle',
          symbolSize: 10,
          itemStyle: { color: '#FF5252' },
          data: vibrationAlertPoints,
        },
      },
      {
        name: 'Temperature',
        type: 'line',
        smooth: true,
        symbol: 'circle',
        symbolSize: 4,
        yAxisIndex: 1,
        lineStyle: { color: '#00BFA5', width: 2 },
        itemStyle: { color: '#00BFA5' },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(0, 191, 165, 0.4)' },
              { offset: 1, color: 'rgba(0, 191, 165, 0.05)' },
            ],
          },
        },
        data: data.map((r) => r.temperature),
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color: '#FF5252', type: 'dashed', width: 1 },
          data: [
            {
              yAxis: TEMPERATURE_THRESHOLD,
              label: {
                formatter: `${TEMPERATURE_THRESHOLD} \u00B0C`,
                color: '#FF5252',
                position: 'insideEndTop',
              },
            },
          ],
        },
        markPoint: {
          symbol: 'circle',
          symbolSize: 10,
          itemStyle: { color: '#FF5252' },
          data: temperatureAlertPoints,
        },
      },
    ],
    animation: true,
    animationDuration: 1000,
  }
})
</script>

<template>
  <div class="trend-chart">
    <v-chart
      v-if="readings.length > 0"
      :option="chartOption"
      style="height: 300px"
      autoresize
    />
    <div
      v-else
      class="d-flex flex-column align-center justify-center text-medium-emphasis"
      style="height: 300px"
    >
      <v-icon size="48" class="mb-2">mdi-chart-timeline-variant</v-icon>
      <div>Waiting for sensor readings...</div>
    </div>
  </div>
</template>

<style scoped>
.trend-chart {
  width: 100%;
  background: rgba(0, 0, 0, 0.3);
  border-radius: 8px;
  padding: 8px;
}
</style>
