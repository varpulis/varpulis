<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EChartsOption } from 'echarts'

const props = defineProps<{
  newsInjected: boolean
  trades: Array<{ amount: number; price: number }>
}>()

// Seed-based pseudo-random for deterministic candle data
function seededRandom(seed: number): () => number {
  let s = seed
  return () => {
    s = (s * 16807 + 0) % 2147483647
    return (s - 1) / 2147483646
  }
}

// Pre-generate 20 synthetic candlestick data points for ACME
// Gradual uptrend from ~$42 to ~$46
interface CandleData {
  date: string
  open: number
  close: number
  low: number
  high: number
  volume: number
}

const rand = seededRandom(42)

function generateCandles(): CandleData[] {
  const candles: CandleData[] = []
  let price = 42.0

  for (let i = 0; i < 20; i++) {
    const trend = 0.2 // gradual uptrend per candle
    const volatility = (rand() - 0.5) * 1.0 // +/- $0.50

    const open = parseFloat(price.toFixed(2))
    const change = trend + volatility
    const close = parseFloat((open + change).toFixed(2))
    const low = parseFloat((Math.min(open, close) - rand() * 0.30).toFixed(2))
    const high = parseFloat((Math.max(open, close) + rand() * 0.30).toFixed(2))
    const volume = Math.floor(50000 + rand() * 150000)

    const day = i + 1
    const dateStr = `2026-01-${day.toString().padStart(2, '0')}`

    candles.push({ date: dateStr, open, close, low, high, volume })

    price = close
  }

  return candles
}

const candleData = generateCandles()

const chartOption = computed<EChartsOption>(() => {
  const dates = candleData.map((c) => c.date)
  const ohlc = candleData.map((c) => [c.open, c.close, c.low, c.high])
  const volumes = candleData.map((c) => ({
    value: c.volume,
    itemStyle: {
      color: c.close >= c.open ? 'rgba(76, 175, 80, 0.5)' : 'rgba(255, 82, 82, 0.5)',
    },
  }))

  const markPointData: Array<Record<string, unknown>> = []
  if (props.newsInjected) {
    markPointData.push({
      name: 'NEWS',
      coord: [dates[dates.length - 1], candleData[candleData.length - 1].high],
      value: 'NEWS',
      symbol: 'pin',
      symbolSize: 50,
      itemStyle: { color: '#FF5252' },
      label: {
        show: true,
        formatter: 'NEWS',
        color: '#fff',
        fontWeight: 'bold',
        fontSize: 11,
      },
    })
  }

  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross',
        crossStyle: { color: '#555' },
      },
      backgroundColor: 'rgba(20, 20, 30, 0.95)',
      borderColor: '#7C4DFF',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: unknown) => {
        const ps = params as Array<{ seriesIndex: number; data: number[] | { value: number }; axisValue: string }>
        if (!ps || ps.length === 0) return ''
        const candle = ps.find((p) => p.seriesIndex === 0)
        if (!candle || !Array.isArray(candle.data)) return ''
        const [open, close, low, high] = candle.data as number[]
        const vol = ps.find((p) => p.seriesIndex === 1)
        const volVal = vol ? (typeof vol.data === 'object' && 'value' in vol.data ? vol.data.value : 0) : 0
        return `<div style="font-weight:600;margin-bottom:4px">${candle.axisValue} - ACME</div>
          O: <b>${open.toFixed(2)}</b> &nbsp; H: <b>${high.toFixed(2)}</b><br/>
          L: <b>${low.toFixed(2)}</b> &nbsp; C: <b>${close.toFixed(2)}</b><br/>
          Vol: <b>${Number(volVal).toLocaleString()}</b>`
      },
    },
    grid: [
      {
        left: '8%',
        right: '3%',
        top: '8%',
        height: '55%',
      },
      {
        left: '8%',
        right: '3%',
        top: '70%',
        height: '16%',
      },
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        gridIndex: 0,
        axisLine: { lineStyle: { color: '#444' } },
        axisLabel: { color: '#9E9E9E', fontSize: 10 },
        splitLine: { show: false },
      },
      {
        type: 'category',
        data: dates,
        gridIndex: 1,
        axisLine: { lineStyle: { color: '#444' } },
        axisLabel: { show: false },
        splitLine: { show: false },
      },
    ],
    yAxis: [
      {
        type: 'value',
        name: 'ACME ($)',
        nameTextStyle: { color: '#9E9E9E', fontSize: 11 },
        gridIndex: 0,
        scale: true,
        axisLine: { show: false },
        axisLabel: { color: '#9E9E9E' },
        splitLine: { lineStyle: { color: 'rgba(84, 110, 122, 0.2)' } },
      },
      {
        type: 'value',
        name: 'Vol',
        nameTextStyle: { color: '#666', fontSize: 10 },
        gridIndex: 1,
        scale: true,
        axisLine: { show: false },
        axisLabel: { show: false },
        splitLine: { show: false },
      },
    ],
    dataZoom: [
      {
        type: 'slider',
        xAxisIndex: [0, 1],
        bottom: '2%',
        height: 20,
        borderColor: '#444',
        backgroundColor: 'rgba(30, 30, 40, 0.8)',
        fillerColor: 'rgba(124, 77, 255, 0.15)',
        handleStyle: { color: '#7C4DFF' },
        textStyle: { color: '#9E9E9E' },
      },
    ],
    series: [
      {
        name: 'ACME',
        type: 'candlestick',
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: ohlc,
        itemStyle: {
          color: '#4CAF50',
          color0: '#FF5252',
          borderColor: '#4CAF50',
          borderColor0: '#FF5252',
        },
        markPoint:
          markPointData.length > 0
            ? { data: markPointData as never[] }
            : undefined,
      },
      {
        name: 'Volume',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        barWidth: '60%',
      },
    ],
    animation: true,
    animationDuration: 600,
  }
})
</script>

<template>
  <div class="price-chart-container">
    <VChart
      :option="chartOption"
      style="height: 100%; width: 100%"
      autoresize
    />
  </div>
</template>

<style scoped>
.price-chart-container {
  height: 100%;
  width: 100%;
  min-height: 300px;
}
</style>
