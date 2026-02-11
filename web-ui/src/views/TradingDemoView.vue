<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import PriceChart from '@/components/demos/trading/PriceChart.vue'
import OrderAccumulator from '@/components/demos/trading/OrderAccumulator.vue'
import { insiderTradingScenario } from '@/data/scenarios/insider-trading'

const newsInjected = ref(false)
const alertFired = ref(false)
const trades = ref<Array<{ amount: number; price: number }>>([])

// Map step index to the trades injected by that step
const stepTrades: Record<number, Array<{ amount: number; price: number }>> = {
  0: [{ amount: 100, price: 50.0 }],
  1: [{ amount: 10000, price: 45.5 }],
  2: [], // NewsRelease only, no trades
  3: [
    { amount: 500, price: 12.0 },
    { amount: 600, price: 12.1 },
    { amount: 700, price: 12.2 },
    { amount: 800, price: 12.3 },
  ],
}

function onStepChanged(index: number) {
  // Accumulate trades from all steps up to and including the current one
  const accumulated: Array<{ amount: number; price: number }> = []
  for (let i = 0; i <= index; i++) {
    const stepOrders = stepTrades[i]
    if (stepOrders) {
      accumulated.push(...stepOrders)
    }
  }
  trades.value = accumulated
}

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    const alertType = String(alert.alert_type || '')

    if (alertType === 'trade_before_news') {
      newsInjected.value = true
    }

    if (alertType === 'abnormal_position_building') {
      alertFired.value = true
    }
  }
}
</script>

<template>
  <DemoShell
    :scenario="insiderTradingScenario"
    @alerts="onAlerts"
    @step-changed="onStepChanged"
  >
    <template #hero>
      <div class="d-flex flex-column h-100" style="gap: 12px">
        <!-- Price Chart: 70% -->
        <div style="flex: 7; min-height: 0">
          <PriceChart
            :news-injected="newsInjected"
            :trades="trades"
          />
        </div>

        <!-- Order Accumulator: 30% -->
        <div style="flex: 3; min-height: 0">
          <v-card
            color="rgba(0, 0, 0, 0.4)"
            variant="flat"
            class="h-100 pa-2"
            style="backdrop-filter: blur(6px); border: 1px solid rgba(255, 255, 255, 0.06)"
          >
            <div class="d-flex align-center px-3 pt-1 pb-0">
              <v-icon size="18" color="purple-lighten-2" class="mr-2">mdi-chart-bar-stacked</v-icon>
              <span class="text-caption text-medium-emphasis font-weight-medium">
                Position Accumulation
              </span>
            </div>
            <OrderAccumulator
              :orders="trades"
              :alert-fired="alertFired"
            />
          </v-card>
        </div>
      </div>
    </template>
  </DemoShell>
</template>
