<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useMetricsStore } from '@/stores/metrics'
import { useClusterStore } from '@/stores/cluster'
import { fetchClusterMetrics } from '@/api/cluster'
import ThroughputChart from '@/components/metrics/ThroughputChart.vue'
import LatencyHistogram from '@/components/metrics/LatencyHistogram.vue'
import MetricCard from '@/components/metrics/MetricCard.vue'
import MetricsGrid from '@/components/metrics/MetricsGrid.vue'
import type { TimeRangePreset } from '@/types/metrics'

const metricsStore = useMetricsStore()
const clusterStore = useClusterStore()

// Track previous values for throughput calculation
let prevEventsIn = 0
let prevTimestamp = Date.now()

const autoRefresh = ref(true)
const refreshInterval = ref(5000)
const fetchError = ref<string | null>(null)
let pollInterval: ReturnType<typeof setInterval> | null = null

const timeRange = computed(() => metricsStore.timeRange.preset)
const aggregated = computed(() => metricsStore.aggregated)
const lastUpdate = computed(() => metricsStore.lastUpdate)

const timeRangeOptions: { title: string; value: TimeRangePreset }[] = [
  { title: '5 minutes', value: '5m' },
  { title: '15 minutes', value: '15m' },
  { title: '1 hour', value: '1h' },
  { title: '6 hours', value: '6h' },
  { title: '24 hours', value: '24h' },
]

function setTimeRange(preset: TimeRangePreset): void {
  metricsStore.setTimeRange(preset)
}

async function fetchMetrics(): Promise<void> {
  try {
    fetchError.value = null
    const clusterMetrics = await fetchClusterMetrics()
    const workers = clusterStore.workers
    const totalPipelines = workers.reduce((sum, w) => sum + w.pipelines_running, 0)

    // Aggregate events_in and events_out across all pipelines
    const totalEventsIn = clusterMetrics.pipelines.reduce((sum, p) => sum + p.events_in, 0)
    const totalEventsOut = clusterMetrics.pipelines.reduce((sum, p) => sum + p.events_out, 0)

    // Calculate throughput from delta between polls
    const now = Date.now()
    const dtSecs = (now - prevTimestamp) / 1000
    const throughput = dtSecs > 0 && prevEventsIn > 0
      ? (totalEventsIn - prevEventsIn) / dtSecs
      : 0
    prevEventsIn = totalEventsIn
    prevTimestamp = now

    metricsStore.updateMetrics({
      events_processed: totalEventsIn,
      events_emitted: totalEventsOut,
      errors: 0,
      active_streams: totalPipelines,
      uptime_secs: metricsStore.aggregated.uptime_secs + (refreshInterval.value / 1000),
      throughput_eps: Math.max(0, throughput),
      avg_latency_ms: 0,
    })
  } catch (e) {
    fetchError.value = e instanceof Error ? e.message : 'Failed to fetch metrics'
  }
}

function toggleAutoRefresh(): void {
  autoRefresh.value = !autoRefresh.value
  if (autoRefresh.value) {
    startPolling()
  } else {
    stopPolling()
  }
}

function startPolling(): void {
  if (pollInterval) {
    clearInterval(pollInterval)
  }
  fetchMetrics() // Fetch immediately
  pollInterval = setInterval(fetchMetrics, refreshInterval.value)
}

function stopPolling(): void {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
}

function refreshNow(): void {
  fetchMetrics()
}

onMounted(async () => {
  // Fetch workers to populate the worker metrics grid
  await clusterStore.fetchWorkers()

  if (autoRefresh.value) {
    startPolling()
  }
})

onUnmounted(() => {
  stopPolling()
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Metrics Dashboard</h1>
      <v-spacer />

      <!-- Time Range Selector -->
      <v-btn-toggle
        :model-value="timeRange"
        mandatory
        density="compact"
        class="mr-4"
        @update:model-value="setTimeRange"
      >
        <v-btn
          v-for="option in timeRangeOptions"
          :key="option.value"
          :value="option.value"
          size="small"
        >
          {{ option.title }}
        </v-btn>
      </v-btn-toggle>

      <!-- Auto Refresh Toggle -->
      <v-btn
        :icon="autoRefresh ? 'mdi-pause' : 'mdi-play'"
        variant="outlined"
        size="small"
        class="mr-2"
        @click="toggleAutoRefresh"
      >
        <v-tooltip activator="parent" location="bottom">
          {{ autoRefresh ? 'Pause auto-refresh' : 'Resume auto-refresh' }}
        </v-tooltip>
      </v-btn>

      <v-btn
        icon="mdi-refresh"
        variant="outlined"
        size="small"
        @click="refreshNow"
      >
        <v-tooltip activator="parent" location="bottom">
          Refresh now
        </v-tooltip>
      </v-btn>
    </div>

    <!-- Error Alert -->
    <v-alert
      v-if="fetchError"
      type="error"
      variant="tonal"
      closable
      class="mb-4"
      @click:close="fetchError = null"
    >
      <v-icon start>mdi-alert-circle</v-icon>
      {{ fetchError }}
    </v-alert>

    <!-- Summary Cards -->
    <v-row class="mb-4">
      <v-col cols="12" sm="6" md="3">
        <MetricCard
          title="Throughput"
          :value="aggregated.throughput_eps"
          unit="evt/s"
          icon="mdi-speedometer"
          color="primary"
          :sparkline-data="metricsStore.throughputData"
        />
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <MetricCard
          title="Avg Latency"
          :value="aggregated.avg_latency_ms"
          unit="ms"
          icon="mdi-timer-outline"
          color="warning"
          :decimals="2"
        />
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <MetricCard
          title="Events Processed"
          :value="aggregated.events_processed_total"
          icon="mdi-chart-timeline-variant"
          color="info"
          :format-number="true"
        />
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <MetricCard
          title="Error Rate"
          :value="metricsStore.errorRate"
          unit="%"
          icon="mdi-alert-circle"
          :color="metricsStore.errorRate > 5 ? 'error' : metricsStore.errorRate > 1 ? 'warning' : 'success'"
          :decimals="2"
        />
      </v-col>
    </v-row>

    <!-- Charts Row -->
    <v-row>
      <v-col cols="12" lg="8">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-chart-line</v-icon>
            Event Throughput
            <v-spacer />
            <span v-if="lastUpdate" class="text-caption text-medium-emphasis">
              Last updated: {{ lastUpdate.toLocaleTimeString() }}
            </span>
          </v-card-title>
          <v-card-text>
            <ThroughputChart :height="350" />
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" lg="4">
        <v-card class="h-100">
          <v-card-title>
            <v-icon class="mr-2">mdi-chart-histogram</v-icon>
            Latency Distribution
          </v-card-title>
          <v-card-text>
            <LatencyHistogram :height="350" />
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Worker Metrics Grid -->
    <v-row class="mt-4">
      <v-col cols="12">
        <MetricsGrid />
      </v-col>
    </v-row>

    <!-- Additional Stats -->
    <v-row class="mt-4">
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-information-outline</v-icon>
            System Stats
          </v-card-title>
          <v-card-text>
            <v-list density="compact">
              <v-list-item>
                <template #prepend>
                  <v-icon>mdi-clock-outline</v-icon>
                </template>
                <v-list-item-title>Uptime</v-list-item-title>
                <template #append>
                  <span class="text-body-2">
                    {{ Math.floor(aggregated.uptime_secs / 3600) }}h
                    {{ Math.floor((aggregated.uptime_secs % 3600) / 60) }}m
                  </span>
                </template>
              </v-list-item>

              <v-list-item>
                <template #prepend>
                  <v-icon>mdi-waves</v-icon>
                </template>
                <v-list-item-title>Active Streams</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ aggregated.active_streams }}</span>
                </template>
              </v-list-item>

              <v-list-item>
                <template #prepend>
                  <v-icon>mdi-arrow-down</v-icon>
                </template>
                <v-list-item-title>Events In</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ aggregated.events_processed_total.toLocaleString() }}</span>
                </template>
              </v-list-item>

              <v-list-item>
                <template #prepend>
                  <v-icon>mdi-arrow-up</v-icon>
                </template>
                <v-list-item-title>Events Out</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ aggregated.events_emitted_total.toLocaleString() }}</span>
                </template>
              </v-list-item>

              <v-list-item>
                <template #prepend>
                  <v-icon color="error">mdi-alert</v-icon>
                </template>
                <v-list-item-title>Errors</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ aggregated.errors_total.toLocaleString() }}</span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-timer-sand</v-icon>
            Latency Percentiles
          </v-card-title>
          <v-card-text>
            <v-list density="compact">
              <v-list-item>
                <v-list-item-title>P50 (Median)</v-list-item-title>
                <template #append>
                  <v-chip size="small" color="success">
                    {{ metricsStore.latencyPercentiles.p50.toFixed(2) }} ms
                  </v-chip>
                </template>
              </v-list-item>

              <v-list-item>
                <v-list-item-title>P90</v-list-item-title>
                <template #append>
                  <v-chip size="small" color="info">
                    {{ metricsStore.latencyPercentiles.p90.toFixed(2) }} ms
                  </v-chip>
                </template>
              </v-list-item>

              <v-list-item>
                <v-list-item-title>P95</v-list-item-title>
                <template #append>
                  <v-chip size="small" color="warning">
                    {{ metricsStore.latencyPercentiles.p95.toFixed(2) }} ms
                  </v-chip>
                </template>
              </v-list-item>

              <v-list-item>
                <v-list-item-title>P99</v-list-item-title>
                <template #append>
                  <v-chip size="small" color="error">
                    {{ metricsStore.latencyPercentiles.p99.toFixed(2) }} ms
                  </v-chip>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>
