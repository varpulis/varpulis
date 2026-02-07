<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { useMetricsStore } from '@/stores/metrics'
import { format } from 'date-fns'
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
  const data = metricsStore.throughputData

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: unknown) => {
        const p = (params as Array<{ value: number[]; seriesName: string }>)[0]
        if (!p || !p.value) return ''
        const time = format(new Date(p.value[0]), 'HH:mm:ss')
        return `${time}<br/>Throughput: <strong>${p.value[1].toFixed(1)}</strong> evt/s`
      },
      backgroundColor: 'rgba(30, 30, 30, 0.9)',
      borderColor: '#7C4DFF',
      textStyle: {
        color: '#fff',
      },
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      top: '10%',
      containLabel: true,
    },
    xAxis: {
      type: 'time',
      axisLine: {
        lineStyle: {
          color: '#546E7A',
        },
      },
      axisLabel: {
        formatter: (value: number) => format(new Date(value), 'HH:mm:ss'),
        color: '#9E9E9E',
      },
      splitLine: {
        show: false,
      },
    },
    yAxis: {
      type: 'value',
      name: 'Events/sec',
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
        name: 'Throughput',
        type: 'line',
        smooth: true,
        symbol: 'none',
        sampling: 'lttb',
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
        lineStyle: {
          color: '#7C4DFF',
          width: 2,
        },
        itemStyle: {
          color: '#7C4DFF',
        },
        data: data.map((d) => [d.timestamp, d.value]),
      },
    ],
    animation: false,
  }
})
</script>

<template>
  <div class="throughput-chart">
    <v-chart
      v-if="metricsStore.throughputData.length > 0"
      :option="chartOption"
      :style="{ height: typeof height === 'number' ? `${height}px` : height }"
      autoresize
    />
    <div
      v-else
      class="d-flex flex-column align-center justify-center text-medium-emphasis"
      :style="{ height: typeof height === 'number' ? `${height}px` : height }"
    >
      <v-icon size="48" class="mb-2">mdi-chart-line-variant</v-icon>
      <div>Waiting for metrics data...</div>
    </div>
  </div>
</template>

<style scoped>
.throughput-chart {
  width: 100%;
}
</style>
