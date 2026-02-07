<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { useMetricsStore } from '@/stores/metrics'
import type { EChartsOption } from 'echarts'

withDefaults(
  defineProps<{
    height?: number | string
  }>(),
  {
    height: 300,
  }
)

const metricsStore = useMetricsStore()

const chartOption = computed<EChartsOption>(() => {
  const { p50, p90, p95, p99 } = metricsStore.latencyPercentiles

  // Create histogram buckets from percentile data
  const buckets = [
    { label: '0-1ms', value: Math.max(0, p50 * 0.5) },
    { label: '1-5ms', value: Math.max(0, p50) },
    { label: '5-10ms', value: Math.max(0, (p50 + p90) / 2) },
    { label: '10-25ms', value: Math.max(0, p90) },
    { label: '25-50ms', value: Math.max(0, p95) },
    { label: '50-100ms', value: Math.max(0, (p95 + p99) / 2) },
    { label: '>100ms', value: Math.max(0, p99) },
  ]

  return {
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'shadow',
      },
      backgroundColor: 'rgba(30, 30, 30, 0.9)',
      borderColor: '#00BFA5',
      textStyle: {
        color: '#fff',
      },
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      top: '15%',
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      data: buckets.map((b) => b.label),
      axisLine: {
        lineStyle: {
          color: '#546E7A',
        },
      },
      axisLabel: {
        color: '#9E9E9E',
        rotate: 30,
      },
    },
    yAxis: {
      type: 'value',
      name: 'Latency (ms)',
      nameTextStyle: {
        color: '#9E9E9E',
      },
      axisLine: {
        show: false,
      },
      axisLabel: {
        color: '#9E9E9E',
      },
      splitLine: {
        lineStyle: {
          color: 'rgba(84, 110, 122, 0.3)',
        },
      },
    },
    series: [
      {
        name: 'Latency',
        type: 'bar',
        barWidth: '60%',
        data: buckets.map((b, index) => ({
          value: b.value,
          itemStyle: {
            color: getBarColor(index),
          },
        })),
      },
    ],
    animation: true,
    animationDuration: 500,
  }
})

function getBarColor(index: number): string {
  const colors = [
    '#4CAF50', // Green - fast
    '#8BC34A',
    '#CDDC39',
    '#FFC107', // Yellow - medium
    '#FF9800',
    '#FF5722', // Orange - slow
    '#F44336', // Red - very slow
  ]
  return colors[index] || colors[colors.length - 1]
}

// Percentile markers
const percentileMarkers = computed(() => [
  { label: 'P50', value: metricsStore.latencyPercentiles.p50, color: '#4CAF50' },
  { label: 'P90', value: metricsStore.latencyPercentiles.p90, color: '#2196F3' },
  { label: 'P95', value: metricsStore.latencyPercentiles.p95, color: '#FFC107' },
  { label: 'P99', value: metricsStore.latencyPercentiles.p99, color: '#F44336' },
])
</script>

<template>
  <div class="latency-histogram">
    <VChart
      :option="chartOption"
      :style="{ height: typeof height === 'number' ? `${height - 60}px` : `calc(${height} - 60px)` }"
      autoresize
    />

    <!-- Percentile Legend -->
    <div class="d-flex justify-center flex-wrap gap-3 mt-2">
      <div
        v-for="marker in percentileMarkers"
        :key="marker.label"
        class="d-flex align-center"
      >
        <div
          class="percentile-dot mr-1"
          :style="{ backgroundColor: marker.color }"
        />
        <span class="text-caption">
          {{ marker.label }}: {{ marker.value.toFixed(2) }}ms
        </span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.latency-histogram {
  width: 100%;
}

.percentile-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
}
</style>
