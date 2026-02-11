<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import type { EChartsOption } from 'echarts'

const props = defineProps<{
  orders: Array<{ amount: number; price: number }>
  alertFired: boolean
}>()

// Generate a gradient of purple shades from light to dark
function orderColor(index: number, total: number): string {
  if (props.alertFired) return '#FF5252'
  if (total <= 1) return '#7C4DFF'
  // Interpolate from light purple to dark purple
  const lightness = 70 - (index / (total - 1)) * 35 // 70% down to 35%
  return `hsl(262, 100%, ${lightness}%)`
}

const totalShares = computed(() =>
  props.orders.reduce((sum, o) => sum + o.amount, 0),
)

const chartOption = computed<EChartsOption>(() => {
  const seriesList = props.orders.map((order, index) => ({
    name: `Order ${index + 1}`,
    type: 'bar' as const,
    stack: 'orders',
    data: [order.amount],
    barWidth: '50%',
    itemStyle: {
      color: orderColor(index, props.orders.length),
      borderRadius: index === props.orders.length - 1 ? [0, 4, 4, 0] : 0,
    },
    label:
      index === props.orders.length - 1
        ? {
            show: true,
            position: 'right' as const,
            formatter: `Total: ${totalShares.value.toLocaleString()} shares`,
            color: '#fff',
            fontWeight: 'bold' as const,
            fontSize: 13,
            distance: 12,
          }
        : { show: false },
  }))

  // If no orders, show empty placeholder
  if (seriesList.length === 0) {
    seriesList.push({
      name: 'Empty',
      type: 'bar',
      stack: 'orders',
      data: [0],
      barWidth: '50%',
      itemStyle: {
        color: 'rgba(124, 77, 255, 0.15)',
        borderRadius: [0, 4, 4, 0],
      },
      label: {
        show: true,
        position: 'right',
        formatter: 'No orders',
        color: '#666',
        fontWeight: 'bold',
        fontSize: 13,
        distance: 12,
      },
    })
  }

  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      backgroundColor: 'rgba(20, 20, 30, 0.95)',
      borderColor: '#7C4DFF',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: unknown) => {
        const p = params as { seriesName: string; value: number }
        if (!p || !p.value) return ''
        return `${p.seriesName}: <b>${p.value.toLocaleString()} shares</b>`
      },
    },
    grid: {
      left: '3%',
      right: '22%',
      top: '20%',
      bottom: '20%',
      containLabel: true,
    },
    xAxis: {
      type: 'value',
      axisLine: { show: false },
      axisLabel: { show: false },
      splitLine: { show: false },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'category',
      data: ['Position'],
      axisLine: { lineStyle: { color: '#444' } },
      axisLabel: {
        color: '#9E9E9E',
        fontWeight: 'bold',
        fontSize: 12,
      },
      axisTick: { show: false },
    },
    series: seriesList,
    animation: true,
    animationDuration: 400,
    animationEasing: 'cubicOut',
  }
})
</script>

<template>
  <div class="order-accumulator" :class="{ 'alert-flash': alertFired }">
    <VChart
      :option="chartOption"
      style="height: 100%; width: 100%"
      autoresize
    />
  </div>
</template>

<style scoped>
.order-accumulator {
  height: 120px;
  width: 100%;
  border-radius: 8px;
  transition: box-shadow 0.3s ease;
}

.alert-flash {
  animation: accumulator-flash 0.8s ease-in-out 3;
}

@keyframes accumulator-flash {
  0%, 100% {
    box-shadow: none;
  }
  50% {
    box-shadow: 0 0 20px rgba(255, 82, 82, 0.6), inset 0 0 10px rgba(255, 82, 82, 0.15);
  }
}
</style>
