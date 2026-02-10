<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { useClusterStore } from '@/stores/cluster'
import { usePipelinesStore } from '@/stores/pipelines'
import { useMetricsStore } from '@/stores/metrics'
import ThroughputChart from '@/components/metrics/ThroughputChart.vue'
import { formatDistanceToNow } from 'date-fns'
import { fetchClusterMetrics, type PipelineWorkerMetrics } from '@/api/cluster'

const router = useRouter()
const clusterStore = useClusterStore()
const pipelinesStore = usePipelinesStore()
const metricsStore = useMetricsStore()

const loading = ref(true)
const pipelineActivity = ref<PipelineWorkerMetrics[]>([])
let pollInterval: ReturnType<typeof setInterval> | null = null
let prevEventsIn = 0
let prevTimestamp = Date.now()

const totalEventsOut = computed(() =>
  pipelineActivity.value.reduce((sum, p) => sum + p.events_out, 0)
)

// Computed summary
const workersSummary = computed(() => ({
  total: clusterStore.workers.length,
  healthy: clusterStore.healthyWorkers.length,
  unhealthy: clusterStore.unhealthyWorkers.length,
  draining: clusterStore.drainingWorkers.length,
}))

const pipelinesSummary = computed(() => ({
  total: pipelinesStore.groups.length,
  running: pipelinesStore.runningGroups.length,
  failed: pipelinesStore.failedGroups.length,
  pending: pipelinesStore.pendingGroups.length,
}))

const metrics = computed(() => metricsStore.aggregated)

const recentAlerts = computed(() => clusterStore.recentAlerts)

// Quick action handlers
function navigateToCluster(): void {
  router.push('/cluster')
}

function navigateToPipelines(): void {
  router.push('/pipelines')
}

function openDeployDialog(): void {
  router.push('/pipelines')
}

function navigateToMetrics(): void {
  router.push('/metrics')
}

// Format timestamp
function formatTime(timestamp: string): string {
  return formatDistanceToNow(new Date(timestamp), { addSuffix: true })
}

// Fetch data
async function fetchData(): Promise<void> {
  await Promise.all([
    clusterStore.fetchWorkers(),
    pipelinesStore.fetchGroups(),
    fetchClusterMetrics()
      .then((m) => {
        pipelineActivity.value = m.pipelines
        // Update metricsStore with throughput from pipeline data
        const totalIn = m.pipelines.reduce((s, p) => s + p.events_in, 0)
        const totalOut = m.pipelines.reduce((s, p) => s + p.events_out, 0)
        const now = Date.now()
        const dtSecs = (now - prevTimestamp) / 1000
        const throughput = dtSecs > 0 && prevEventsIn > 0
          ? (totalIn - prevEventsIn) / dtSecs : 0
        prevEventsIn = totalIn
        prevTimestamp = now
        const workers = clusterStore.workers
        const totalPipelines = workers.reduce((s, w) => s + w.pipelines_running, 0)
        metricsStore.updateMetrics({
          events_processed: totalIn,
          events_emitted: totalOut,
          errors: 0,
          active_streams: totalPipelines,
          uptime_secs: metricsStore.aggregated.uptime_secs + 5,
          throughput_eps: Math.max(0, throughput),
          avg_latency_ms: 0,
        })
      })
      .catch(() => { /* metrics endpoint may not exist yet */ }),
  ])
  loading.value = false
}

onMounted(() => {
  fetchData()
  // Poll every 5 seconds
  pollInterval = setInterval(fetchData, 5000)
})

onUnmounted(() => {
  if (pollInterval) {
    clearInterval(pollInterval)
  }
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Dashboard</h1>
      <v-spacer />
      <v-btn
        color="primary"
        prepend-icon="mdi-plus"
        @click="openDeployDialog"
      >
        Deploy Pipeline
      </v-btn>
    </div>

    <!-- Summary Cards Row -->
    <v-row>
      <!-- Workers Card -->
      <v-col cols="12" md="6" lg="3">
        <v-card class="h-100" @click="navigateToCluster" style="cursor: pointer">
          <v-card-text>
            <div class="d-flex align-center mb-2">
              <v-icon color="primary" size="32">mdi-server-network</v-icon>
              <span class="text-h6 ml-2">Workers</span>
            </div>
            <div class="text-h3 font-weight-bold">
              {{ workersSummary.total }}
            </div>
            <div class="d-flex gap-2 mt-2">
              <v-chip color="success" size="x-small" variant="flat">
                {{ workersSummary.healthy }} healthy
              </v-chip>
              <v-chip v-if="workersSummary.unhealthy > 0" color="error" size="x-small" variant="flat">
                {{ workersSummary.unhealthy }} unhealthy
              </v-chip>
              <v-chip v-if="workersSummary.draining > 0" color="warning" size="x-small" variant="flat">
                {{ workersSummary.draining }} draining
              </v-chip>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Pipeline Groups Card -->
      <v-col cols="12" md="6" lg="3">
        <v-card class="h-100" @click="navigateToPipelines" style="cursor: pointer">
          <v-card-text>
            <div class="d-flex align-center mb-2">
              <v-icon color="secondary" size="32">mdi-pipe</v-icon>
              <span class="text-h6 ml-2">Pipeline Groups</span>
            </div>
            <div class="text-h3 font-weight-bold">
              {{ pipelinesSummary.total }}
            </div>
            <div class="d-flex gap-2 mt-2">
              <v-chip color="success" size="x-small" variant="flat">
                {{ pipelinesSummary.running }} running
              </v-chip>
              <v-chip v-if="pipelinesSummary.failed > 0" color="error" size="x-small" variant="flat">
                {{ pipelinesSummary.failed }} failed
              </v-chip>
              <v-chip v-if="pipelinesSummary.pending > 0" color="info" size="x-small" variant="flat">
                {{ pipelinesSummary.pending }} pending
              </v-chip>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Throughput Card -->
      <v-col cols="12" md="6" lg="3">
        <v-card class="h-100" @click="navigateToMetrics" style="cursor: pointer">
          <v-card-text>
            <div class="d-flex align-center mb-2">
              <v-icon color="info" size="32">mdi-speedometer</v-icon>
              <span class="text-h6 ml-2">Throughput</span>
            </div>
            <div class="text-h3 font-weight-bold">
              {{ metrics.throughput_eps.toFixed(1) }}
              <span class="text-h6 text-medium-emphasis">evt/s</span>
            </div>
            <div class="text-caption text-medium-emphasis mt-2">
              {{ metrics.events_processed_total.toLocaleString() }} total events processed
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Events Emitted Card -->
      <v-col cols="12" md="6" lg="3">
        <v-card class="h-100" @click="navigateToMetrics" style="cursor: pointer">
          <v-card-text>
            <div class="d-flex align-center mb-2">
              <v-icon color="warning" size="32">mdi-arrow-up-bold</v-icon>
              <span class="text-h6 ml-2">Events Out</span>
            </div>
            <div class="text-h3 font-weight-bold">
              {{ totalEventsOut.toLocaleString() }}
            </div>
            <div class="text-caption text-medium-emphasis mt-2">
              {{ metrics.active_streams }} active streams
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Throughput Chart & Alerts Row -->
    <v-row class="mt-2">
      <!-- Throughput Chart -->
      <v-col cols="12" lg="8">
        <v-card>
          <v-card-title class="d-flex align-center">
            <v-icon class="mr-2">mdi-chart-line</v-icon>
            Event Throughput
          </v-card-title>
          <v-card-text>
            <ThroughputChart :height="250" />
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Recent Alerts -->
      <v-col cols="12" lg="4">
        <v-card class="h-100">
          <v-card-title class="d-flex align-center">
            <v-icon class="mr-2">mdi-bell-outline</v-icon>
            Recent Alerts
            <v-spacer />
            <v-badge
              v-if="recentAlerts.length > 0"
              :content="recentAlerts.length"
              color="error"
              inline
            />
          </v-card-title>
          <v-card-text class="pa-0">
            <v-list v-if="recentAlerts.length > 0" density="compact">
              <v-list-item
                v-for="alert in recentAlerts"
                :key="alert.id"
                :class="{ 'bg-surface-light': !alert.acknowledged }"
              >
                <template #prepend>
                  <v-icon
                    :color="alert.severity === 'critical' || alert.severity === 'error' ? 'error' : alert.severity === 'warning' ? 'warning' : 'info'"
                    size="small"
                  >
                    {{ alert.severity === 'critical' ? 'mdi-alert-octagon' : alert.severity === 'error' ? 'mdi-alert-circle' : alert.severity === 'warning' ? 'mdi-alert' : 'mdi-information' }}
                  </v-icon>
                </template>
                <v-list-item-title class="text-body-2">
                  {{ alert.title }}
                </v-list-item-title>
                <v-list-item-subtitle class="text-caption">
                  {{ formatTime(alert.timestamp) }} - {{ alert.source }}
                </v-list-item-subtitle>
              </v-list-item>
            </v-list>
            <div v-else class="pa-4 text-center text-medium-emphasis">
              <v-icon size="48" class="mb-2">mdi-check-circle-outline</v-icon>
              <div>No recent alerts</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Pipeline Activity -->
    <v-row v-if="pipelineActivity.length > 0" class="mt-2">
      <v-col cols="12">
        <v-card>
          <v-card-title class="d-flex align-center">
            <v-icon class="mr-2">mdi-pipe</v-icon>
            Pipeline Activity
          </v-card-title>
          <v-card-text class="pa-0">
            <v-table density="compact">
              <thead>
                <tr>
                  <th>Pipeline</th>
                  <th>Worker</th>
                  <th class="text-right">Events In</th>
                  <th class="text-right">Events Out</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in pipelineActivity" :key="row.pipeline_name + row.worker_id">
                  <td><code>{{ row.pipeline_name }}</code></td>
                  <td>
                    <v-chip size="x-small" variant="tonal">{{ row.worker_id }}</v-chip>
                  </td>
                  <td class="text-right">{{ row.events_in.toLocaleString() }}</td>
                  <td class="text-right">{{ row.events_out.toLocaleString() }}</td>
                </tr>
              </tbody>
            </v-table>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Quick Actions -->
    <v-row class="mt-2">
      <v-col cols="12">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-lightning-bolt</v-icon>
            Quick Actions
          </v-card-title>
          <v-card-text>
            <div class="d-flex flex-wrap gap-2">
              <v-btn
                variant="outlined"
                prepend-icon="mdi-plus"
                @click="openDeployDialog"
              >
                Deploy Pipeline Group
              </v-btn>
              <v-btn
                variant="outlined"
                prepend-icon="mdi-graph"
                @click="navigateToCluster"
              >
                View Topology
              </v-btn>
              <v-btn
                variant="outlined"
                prepend-icon="mdi-code-braces"
                @click="router.push('/editor')"
              >
                Open Editor
              </v-btn>
              <v-btn
                variant="outlined"
                prepend-icon="mdi-chart-areaspline"
                @click="navigateToMetrics"
              >
                View Metrics
              </v-btn>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Loading overlay -->
    <v-overlay
      :model-value="loading"
      class="align-center justify-center"
      contained
    >
      <v-progress-circular indeterminate size="64" />
    </v-overlay>
  </div>
</template>
