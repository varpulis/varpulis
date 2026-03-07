<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useOrgStore } from '@/stores/org'
import { useMetricsStore } from '@/stores/metrics'
import { usePipelinesStore } from '@/stores/pipelines'
import { useClusterStore } from '@/stores/cluster'
import { fetchClusterMetrics } from '@/api/cluster'
import api from '@/api'

const orgStore = useOrgStore()
const metricsStore = useMetricsStore()
const pipelinesStore = usePipelinesStore()
const clusterStore = useClusterStore()

const loading = ref(true)
const usageData = ref<{ events_processed: number; output_events_emitted: number } | null>(null)
const orgLimits = ref<{
  pipeline_limit: number
  events_per_second_limit: number
  monthly_event_limit: number
  events_this_month: number
} | null>(null)

let pollInterval: ReturnType<typeof setInterval> | null = null
let prevEventsIn = -1
let prevTimestamp = Date.now()

const currentEps = computed(() => metricsStore.aggregated.throughput_eps)

const monthlyUsagePercent = computed(() => {
  if (!orgLimits.value || orgLimits.value.monthly_event_limit <= 0) return 0
  return Math.min(100, (orgLimits.value.events_this_month / orgLimits.value.monthly_event_limit) * 100)
})

const pipelineUsagePercent = computed(() => {
  if (!orgLimits.value || orgLimits.value.pipeline_limit <= 0) return 0
  return Math.min(100, (pipelinesStore.groups.length / orgLimits.value.pipeline_limit) * 100)
})

const usageColor = computed(() => {
  const p = monthlyUsagePercent.value
  if (p >= 90) return 'error'
  if (p >= 70) return 'warning'
  return 'success'
})

const epsColor = computed(() => {
  if (!orgLimits.value || orgLimits.value.events_per_second_limit <= 0) return 'primary'
  const ratio = currentEps.value / orgLimits.value.events_per_second_limit
  if (ratio >= 0.9) return 'error'
  if (ratio >= 0.7) return 'warning'
  return 'primary'
})

const epsPercent = computed(() => {
  if (!orgLimits.value || orgLimits.value.events_per_second_limit <= 0) return 0
  return Math.min(100, (currentEps.value / orgLimits.value.events_per_second_limit) * 100)
})

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + 'B'
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return n.toLocaleString()
}

async function fetchUsage() {
  try {
    const res = await api.get('/usage')
    usageData.value = res.data
  } catch {
    // usage endpoint may not be available
  }
}

async function fetchOrgLimits() {
  if (!orgStore.currentOrg) return
  try {
    const res = await api.get(`/admin/tenants/${orgStore.currentOrg.id}`)
    const d = res.data
    orgLimits.value = {
      pipeline_limit: d.pipeline_limit ?? 10,
      events_per_second_limit: d.events_per_second_limit ?? 10000,
      monthly_event_limit: d.monthly_event_limit ?? 10_000_000,
      events_this_month: d.events_this_month ?? 0,
    }
  } catch {
    // fallback defaults
    orgLimits.value = {
      pipeline_limit: 10,
      events_per_second_limit: 10000,
      monthly_event_limit: 10_000_000,
      events_this_month: 0,
    }
  }
}

async function pollMetrics() {
  try {
    const m = await fetchClusterMetrics()
    const pipelines = m.pipelines || []
    const totalIn = pipelines.reduce((s, p) => s + p.events_in, 0)
    const now = Date.now()
    const dtSecs = (now - prevTimestamp) / 1000
    let throughput = 0
    if (prevEventsIn >= 0 && dtSecs > 0) {
      throughput = Math.max(0, (totalIn - prevEventsIn) / dtSecs)
    }
    prevEventsIn = totalIn
    prevTimestamp = now
    const totalOut = pipelines.reduce((s, p) => s + p.events_out, 0)
    metricsStore.updateMetrics({
      events_processed: totalIn,
      events_emitted: totalOut,
      errors: 0,
      active_streams: pipelines.length,
      uptime_secs: metricsStore.aggregated.uptime_secs + 5,
      throughput_eps: throughput,
      avg_latency_ms: 0,
    })
  } catch {
    // metrics not available
  }
}

async function fetchAll() {
  await Promise.all([
    fetchUsage(),
    fetchOrgLimits(),
    pipelinesStore.fetchGroups(),
    clusterStore.fetchWorkers(),
    pollMetrics(),
  ])
  loading.value = false
}

onMounted(() => {
  orgStore.loadOrgs()
  fetchAll()
  pollInterval = setInterval(pollMetrics, 5000)
})

onUnmounted(() => {
  if (pollInterval) clearInterval(pollInterval)
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Usage</h1>
      <v-spacer />
      <v-chip v-if="orgStore.currentOrg" variant="tonal" color="primary" size="small">
        {{ orgStore.currentOrg.name }}
        <template v-if="orgStore.currentOrg.tier">
          &middot; {{ orgStore.currentOrg.tier }}
        </template>
      </v-chip>
    </div>

    <!-- EPS Gauge Row -->
    <v-row>
      <v-col cols="12" md="4">
        <v-card class="text-center pa-4">
          <div class="text-overline mb-2">Current Throughput</div>
          <v-progress-circular
            :model-value="epsPercent"
            :color="epsColor"
            :size="140"
            :width="14"
            class="mb-2"
          >
            <div>
              <div class="text-h4 font-weight-bold">{{ currentEps.toFixed(0) }}</div>
              <div class="text-caption">evt/s</div>
            </div>
          </v-progress-circular>
          <div v-if="orgLimits" class="text-caption text-medium-emphasis">
            Limit: {{ formatNumber(orgLimits.events_per_second_limit) }} evt/s
          </div>
        </v-card>
      </v-col>

      <v-col cols="12" md="4">
        <v-card class="pa-4">
          <div class="text-overline mb-2">Monthly Events</div>
          <div class="d-flex align-center mb-2">
            <span class="text-h4 font-weight-bold">
              {{ orgLimits ? formatNumber(orgLimits.events_this_month) : '...' }}
            </span>
            <span v-if="orgLimits" class="text-body-2 text-medium-emphasis ml-2">
              / {{ formatNumber(orgLimits.monthly_event_limit) }}
            </span>
          </div>
          <v-progress-linear
            :model-value="monthlyUsagePercent"
            :color="usageColor"
            height="12"
            rounded
            class="mb-2"
          />
          <div class="text-caption text-medium-emphasis">
            {{ monthlyUsagePercent.toFixed(1) }}% of monthly quota used
          </div>
        </v-card>
      </v-col>

      <v-col cols="12" md="4">
        <v-card class="pa-4">
          <div class="text-overline mb-2">Pipelines</div>
          <div class="d-flex align-center mb-2">
            <span class="text-h4 font-weight-bold">{{ pipelinesStore.groups.length }}</span>
            <span v-if="orgLimits" class="text-body-2 text-medium-emphasis ml-2">
              / {{ orgLimits.pipeline_limit }}
            </span>
          </div>
          <v-progress-linear
            :model-value="pipelineUsagePercent"
            :color="pipelineUsagePercent >= 90 ? 'error' : pipelineUsagePercent >= 70 ? 'warning' : 'primary'"
            height="12"
            rounded
            class="mb-2"
          />
          <div class="d-flex gap-2 mt-2">
            <v-chip color="success" size="x-small" variant="flat">
              {{ pipelinesStore.runningGroups.length }} running
            </v-chip>
            <v-chip v-if="pipelinesStore.failedGroups.length > 0" color="error" size="x-small" variant="flat">
              {{ pipelinesStore.failedGroups.length }} failed
            </v-chip>
          </div>
        </v-card>
      </v-col>
    </v-row>

    <!-- Cluster Resources -->
    <v-row class="mt-2">
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-server-network</v-icon>
            Cluster Resources
          </v-card-title>
          <v-card-text>
            <v-list density="compact">
              <v-list-item>
                <template #prepend>
                  <v-icon color="success">mdi-server</v-icon>
                </template>
                <v-list-item-title>Workers</v-list-item-title>
                <template #append>
                  <span class="text-body-1 font-weight-medium">{{ clusterStore.workers.length }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <template #prepend>
                  <v-icon color="info">mdi-pipe</v-icon>
                </template>
                <v-list-item-title>Active Streams</v-list-item-title>
                <template #append>
                  <span class="text-body-1 font-weight-medium">{{ metricsStore.aggregated.active_streams }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <template #prepend>
                  <v-icon color="primary">mdi-counter</v-icon>
                </template>
                <v-list-item-title>Total Events Processed</v-list-item-title>
                <template #append>
                  <span class="text-body-1 font-weight-medium">{{ formatNumber(metricsStore.aggregated.events_processed_total) }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <template #prepend>
                  <v-icon color="warning">mdi-arrow-up-bold</v-icon>
                </template>
                <v-list-item-title>Events Emitted</v-list-item-title>
                <template #append>
                  <span class="text-body-1 font-weight-medium">{{ formatNumber(metricsStore.aggregated.events_emitted_total) }}</span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-information</v-icon>
            Plan Details
          </v-card-title>
          <v-card-text>
            <v-list density="compact">
              <v-list-item>
                <v-list-item-title>Organization</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ orgStore.currentOrg?.name ?? 'N/A' }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title>Tier</v-list-item-title>
                <template #append>
                  <v-chip
                    :color="orgStore.currentOrg?.tier === 'enterprise' ? 'purple' : orgStore.currentOrg?.tier === 'business' ? 'info' : orgStore.currentOrg?.tier === 'pro' ? 'success' : 'default'"
                    size="small"
                    variant="tonal"
                  >
                    {{ orgStore.currentOrg?.tier ?? 'free' }}
                  </v-chip>
                </template>
              </v-list-item>
              <v-list-item v-if="orgLimits">
                <v-list-item-title>EPS Limit</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ formatNumber(orgLimits.events_per_second_limit) }} evt/s</span>
                </template>
              </v-list-item>
              <v-list-item v-if="orgLimits">
                <v-list-item-title>Monthly Event Limit</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ formatNumber(orgLimits.monthly_event_limit) }}</span>
                </template>
              </v-list-item>
              <v-list-item v-if="orgLimits">
                <v-list-item-title>Pipeline Limit</v-list-item-title>
                <template #append>
                  <span class="text-body-2">{{ orgLimits.pipeline_limit }}</span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-overlay :model-value="loading" class="align-center justify-center" contained>
      <v-progress-circular indeterminate size="64" />
    </v-overlay>
  </div>
</template>
