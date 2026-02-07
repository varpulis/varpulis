<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { DataPoint } from '@/types/metrics'
import type { EChartsOption } from 'echarts'

const props = withDefaults(
  defineProps<{
    title: string
    value: number
    unit?: string
    icon?: string
    color?: string
    decimals?: number
    formatNumber?: boolean
    sparklineData?: DataPoint[]
  }>(),
  {
    unit: '',
    icon: 'mdi-chart-line',
    color: 'primary',
    decimals: 1,
    formatNumber: false,
  }
)

const formattedValue = computed(() => {
  if (props.formatNumber) {
    return props.value.toLocaleString()
  }
  return props.value.toFixed(props.decimals)
})

const sparklineOption = computed<EChartsOption | null>(() => {
  if (!props.sparklineData || props.sparklineData.length < 2) {
    return null
  }

  const colorMap: Record<string, string> = {
    primary: '#7C4DFF',
    secondary: '#00BFA5',
    success: '#4CAF50',
    warning: '#FFC107',
    error: '#FF5252',
    info: '#2196F3',
  }

  const lineColor = colorMap[props.color] || colorMap.primary

  return {
    grid: {
      left: 0,
      right: 0,
      top: 0,
      bottom: 0,
    },
    xAxis: {
      type: 'time',
      show: false,
    },
    yAxis: {
      type: 'value',
      show: false,
    },
    series: [
      {
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: {
          color: lineColor,
          width: 2,
        },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: `${lineColor}40` },
              { offset: 1, color: `${lineColor}05` },
            ],
          },
        },
        data: props.sparklineData.map((d) => [d.timestamp, d.value]),
      },
    ],
    animation: false,
  }
})
</script>

<template>
  <v-card class="metric-card">
    <v-card-text>
      <div class="d-flex align-center mb-2">
        <v-avatar :color="color" size="36" class="mr-2">
          <v-icon size="20">{{ icon }}</v-icon>
        </v-avatar>
        <span class="text-body-2 text-medium-emphasis">{{ title }}</span>
      </div>

      <div class="d-flex align-end">
        <span class="text-h4 font-weight-bold">{{ formattedValue }}</span>
        <span v-if="unit" class="text-body-2 text-medium-emphasis ml-1 mb-1">
          {{ unit }}
        </span>
      </div>

      <!-- Sparkline -->
      <div v-if="sparklineOption" class="sparkline-container mt-2">
        <VChart
          :option="sparklineOption"
          style="height: 40px"
          autoresize
        />
      </div>
    </v-card-text>
  </v-card>
</template>

<style scoped>
.metric-card {
  height: 100%;
}

.sparkline-container {
  margin: 0 -16px -16px -16px;
}
</style>
