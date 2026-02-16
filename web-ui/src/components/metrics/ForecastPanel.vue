<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { useMetricsStore } from '@/stores/metrics'
import { format } from 'date-fns'
import type { EChartsOption } from 'echarts'

const metricsStore = useMetricsStore()

const hasForecastData = computed(() => metricsStore.forecastData.length > 0)
const latestProbability = computed(() => {
  const data = metricsStore.forecastData
  return data.length > 0 ? data[data.length - 1].value : 0
})

const gaugeOption = computed<EChartsOption>(() => ({
  series: [
    {
      type: 'gauge',
      startAngle: 200,
      endAngle: -20,
      min: 0,
      max: 1,
      splitNumber: 5,
      axisLine: {
        lineStyle: {
          width: 16,
          color: [
            [0.3, '#F44336'],
            [0.6, '#FF9800'],
            [0.8, '#7C4DFF'],
            [1, '#4CAF50'],
          ],
        },
      },
      pointer: {
        itemStyle: { color: '#7C4DFF' },
        width: 4,
      },
      axisTick: { show: false },
      splitLine: {
        length: 8,
        lineStyle: { color: 'auto', width: 2 },
      },
      axisLabel: {
        color: '#9E9E9E',
        fontSize: 10,
        formatter: (value: number) => value.toFixed(1),
      },
      title: {
        offsetCenter: [0, '70%'],
        fontSize: 12,
        color: '#9E9E9E',
      },
      detail: {
        fontSize: 24,
        offsetCenter: [0, '40%'],
        formatter: (value: number) => value.toFixed(3),
        color: '#EEFFFF',
      },
      data: [{ value: latestProbability.value, name: 'Probability' }],
    },
  ],
  animation: false,
}))

const chartOption = computed<EChartsOption>(() => {
  const data = metricsStore.forecastData
  const interval = metricsStore.forecastInterval

  const series: EChartsOption['series'] = [
    {
      name: 'Probability',
      type: 'line',
      smooth: true,
      symbol: 'none',
      lineStyle: { color: '#7C4DFF', width: 2 },
      itemStyle: { color: '#7C4DFF' },
      areaStyle: {
        color: {
          type: 'linear',
          x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(124, 77, 255, 0.3)' },
            { offset: 1, color: 'rgba(124, 77, 255, 0.05)' },
          ],
        },
      },
      data: data.map((d) => [d.timestamp, d.value]),
    },
  ]

  // Add confidence band if we have interval data
  if (interval.upper > 0) {
    series.push(
      {
        name: 'Upper',
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: { opacity: 0 },
        data: data.map((d) => [d.timestamp, Math.min(1, interval.upper)]),
        stack: 'confidence',
      },
      {
        name: 'Lower',
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: { opacity: 0 },
        areaStyle: { color: 'rgba(124, 77, 255, 0.12)' },
        data: data.map((d) => [d.timestamp, Math.max(0, interval.lower)]),
        stack: 'confidence',
      },
    )
  }

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: unknown) => {
        const p = (params as Array<{ value: number[]; seriesName: string }>)[0]
        if (!p || !p.value) return ''
        const time = format(new Date(p.value[0]), 'HH:mm:ss')
        return `${time}<br/>Probability: <strong>${p.value[1].toFixed(3)}</strong>`
      },
      backgroundColor: 'rgba(30, 30, 30, 0.9)',
      borderColor: '#7C4DFF',
      textStyle: { color: '#fff' },
    },
    grid: {
      left: '3%', right: '4%', bottom: '3%', top: '10%',
      containLabel: true,
    },
    xAxis: {
      type: 'time',
      axisLine: { lineStyle: { color: '#546E7A' } },
      axisLabel: {
        formatter: (value: number) => format(new Date(value), 'HH:mm:ss'),
        color: '#9E9E9E',
      },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      name: 'Probability',
      min: 0,
      max: 1,
      nameTextStyle: { color: '#9E9E9E' },
      axisLine: { show: false },
      axisLabel: { color: '#9E9E9E' },
      splitLine: { lineStyle: { color: 'rgba(84, 110, 122, 0.3)' } },
    },
    series,
    animation: false,
  }
})
</script>

<template>
  <div v-if="hasForecastData">
    <v-row>
      <!-- Probability Gauge -->
      <v-col cols="12" md="4">
        <v-card>
          <v-card-title class="text-body-1">
            <v-icon class="mr-2" color="purple">mdi-crystal-ball</v-icon>
            Forecast Probability
          </v-card-title>
          <v-card-text class="d-flex justify-center">
            <v-chart :option="gaugeOption" style="height: 200px; width: 100%" autoresize />
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Confidence & State -->
      <v-col cols="12" md="4">
        <v-card class="h-100">
          <v-card-title class="text-body-1">
            <v-icon class="mr-2" color="purple">mdi-shield-check</v-icon>
            Confidence
          </v-card-title>
          <v-card-text>
            <div class="text-h3 font-weight-bold text-center mb-4">
              {{ (metricsStore.forecastConfidence * 100).toFixed(1) }}%
            </div>
            <v-progress-linear
              :model-value="metricsStore.forecastConfidence * 100"
              color="purple"
              height="12"
              rounded
              class="mb-4"
            />
            <v-list density="compact">
              <v-list-item>
                <v-list-item-title class="text-caption">State</v-list-item-title>
                <template #append>
                  <v-chip size="small" color="purple" variant="tonal">
                    {{ metricsStore.forecastState || 'N/A' }}
                  </v-chip>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title class="text-caption">Interval Width</v-list-item-title>
                <template #append>
                  <span class="text-body-2">
                    {{ (metricsStore.forecastInterval.upper - metricsStore.forecastInterval.lower).toFixed(3) }}
                  </span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Summary Stats -->
      <v-col cols="12" md="4">
        <v-card class="h-100">
          <v-card-title class="text-body-1">
            <v-icon class="mr-2" color="purple">mdi-chart-timeline-variant</v-icon>
            Forecast Summary
          </v-card-title>
          <v-card-text>
            <v-list density="compact">
              <v-list-item>
                <v-list-item-title class="text-caption">Latest Probability</v-list-item-title>
                <template #append>
                  <span class="text-body-2 font-weight-bold">{{ latestProbability.toFixed(3) }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title class="text-caption">Avg Probability</v-list-item-title>
                <template #append>
                  <span class="text-body-2">
                    {{ metricsStore.forecastData.length > 0
                        ? (metricsStore.forecastData.reduce((s, d) => s + d.value, 0) / metricsStore.forecastData.length).toFixed(3)
                        : '0.000'
                    }}
                  </span>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title class="text-caption">Confidence Band</v-list-item-title>
                <template #append>
                  <span class="text-body-2">
                    [{{ metricsStore.forecastInterval.lower.toFixed(2) }},
                     {{ metricsStore.forecastInterval.upper.toFixed(2) }}]
                  </span>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title class="text-caption">Data Points</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ metricsStore.forecastData.length }}</span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Probability Over Time Chart -->
    <v-row class="mt-2">
      <v-col cols="12">
        <v-card>
          <v-card-title class="text-body-1">
            <v-icon class="mr-2" color="purple">mdi-chart-bell-curve-cumulative</v-icon>
            Forecast Probability Over Time
          </v-card-title>
          <v-card-text>
            <v-chart :option="chartOption" style="height: 250px" autoresize />
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>
