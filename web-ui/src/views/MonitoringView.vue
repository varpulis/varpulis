<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'
import { usePipelinesStore } from '@/stores/pipelines'
import { fetchClusterMetrics, type PipelineWorkerMetrics, type ConnectorHealthInfo } from '@/api/cluster'
import { getWebSocketClient } from '@/api/websocket'
import type { ServerMessage } from '@/types/websocket'
import ThroughputChart from '@/components/metrics/ThroughputChart.vue'

const clusterStore = useClusterStore()
const pipelinesStore = usePipelinesStore()

// Pipeline metrics state
const pipelines = ref<PipelineWorkerMetrics[]>([])
const prevPipelineEvents = new Map<string, { events_in: number; timestamp: number }>()
const pipelineThroughputs = ref<Map<string, number>>(new Map())
const loading = ref(true)
const fetchError = ref<string | null>(null)
const autoRefresh = ref(true)
let pollInterval: ReturnType<typeof setInterval> | null = null

// Live event stream state
const liveEvents = ref<LiveEvent[]>([])
const maxLiveEvents = 200
const eventStreamEnabled = ref(false)
const eventStreamPaused = ref(false)
let unsubscribeWs: (() => void) | null = null

// Selected pipeline for detail view
const selectedPipeline = ref<string | null>(null)
const detailDialog = ref(false)

interface LiveEvent {
  id: number
  timestamp: Date
  event_type: string
  pipeline_id?: string
  data: Record<string, unknown>
}

let eventIdCounter = 0

// Computed
const selectedPipelineMetrics = computed(() =>
  pipelines.value.find(p => p.pipeline_name === selectedPipeline.value)
)

const selectedPipelineConnectors = computed(() =>
  selectedPipelineMetrics.value?.connector_health ?? []
)

const totalPipelines = computed(() => pipelines.value.length)

const healthyConnectors = computed(() =>
  pipelines.value.flatMap(p => p.connector_health).filter(c => c.connected).length
)

const unhealthyConnectors = computed(() =>
  pipelines.value.flatMap(p => p.connector_health).filter(c => !c.connected).length
)

const allConnectors = computed(() => {
  const connectors: (ConnectorHealthInfo & { pipeline_name: string; worker_id: string })[] = []
  for (const p of pipelines.value) {
    for (const c of p.connector_health) {
      connectors.push({ ...c, pipeline_name: p.pipeline_name, worker_id: p.worker_id })
    }
  }
  return connectors
})

// Fetch pipeline metrics
async function fetchData(): Promise<void> {
  try {
    fetchError.value = null
    const [metricsData] = await Promise.all([
      fetchClusterMetrics(),
      clusterStore.fetchWorkers(),
      pipelinesStore.fetchGroups(),
    ])

    const now = Date.now()
    const newThroughputs = new Map<string, number>()

    for (const p of metricsData.pipelines) {
      const key = `${p.pipeline_name}@${p.worker_id}`
      const prev = prevPipelineEvents.get(key)
      if (prev) {
        const dtSecs = (now - prev.timestamp) / 1000
        if (dtSecs > 0) {
          newThroughputs.set(key, Math.max(0, (p.events_in - prev.events_in) / dtSecs))
        }
      }
      prevPipelineEvents.set(key, { events_in: p.events_in, timestamp: now })
    }

    pipelines.value = metricsData.pipelines
    pipelineThroughputs.value = newThroughputs
    loading.value = false
  } catch (e) {
    fetchError.value = e instanceof Error ? e.message : 'Failed to fetch metrics'
    loading.value = false
  }
}

function getPipelineThroughput(pipelineName: string, workerId: string): number {
  return pipelineThroughputs.value.get(`${pipelineName}@${workerId}`) ?? 0
}

// Pipeline detail
function showPipelineDetail(pipelineName: string): void {
  selectedPipeline.value = pipelineName
  detailDialog.value = true
}

// Live event stream
function toggleEventStream(): void {
  if (eventStreamEnabled.value) {
    stopEventStream()
  } else {
    startEventStream()
  }
}

function startEventStream(): void {
  const ws = getWebSocketClient()
  ws.connect()

  unsubscribeWs = ws.onMessage((message: ServerMessage) => {
    if (eventStreamPaused.value) return
    if (message.type === 'output_event') {
      liveEvents.value.unshift({
        id: ++eventIdCounter,
        timestamp: new Date(message.timestamp),
        event_type: message.event_type,
        pipeline_id: message.pipeline_id,
        data: message.data,
      })
      if (liveEvents.value.length > maxLiveEvents) {
        liveEvents.value = liveEvents.value.slice(0, maxLiveEvents)
      }
    }
  })

  eventStreamEnabled.value = true
}

function stopEventStream(): void {
  if (unsubscribeWs) {
    unsubscribeWs()
    unsubscribeWs = null
  }
  eventStreamEnabled.value = false
}

function clearEvents(): void {
  liveEvents.value = []
}

function connectorIcon(type: string): string {
  switch (type) {
    case 'nats': return 'mdi-lightning-bolt'
    case 'mqtt': return 'mdi-access-point'
    case 'kafka': return 'mdi-apache-kafka'
    case 'http': return 'mdi-web'
    default: return 'mdi-connection'
  }
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3600)}h ago`
}

// Polling
function startPolling(): void {
  if (pollInterval) clearInterval(pollInterval)
  fetchData()
  pollInterval = setInterval(fetchData, 5000)
}

function stopPolling(): void {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
}

onMounted(() => {
  if (autoRefresh.value) startPolling()
})

onUnmounted(() => {
  stopPolling()
  stopEventStream()
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Pipeline Monitoring</h1>
      <v-spacer />
      <v-btn
        :icon="autoRefresh ? 'mdi-pause' : 'mdi-play'"
        variant="outlined"
        size="small"
        class="mr-2"
        @click="autoRefresh = !autoRefresh; autoRefresh ? startPolling() : stopPolling()"
      >
        <v-tooltip activator="parent" location="bottom">
          {{ autoRefresh ? 'Pause' : 'Resume' }} auto-refresh
        </v-tooltip>
      </v-btn>
      <v-btn icon="mdi-refresh" variant="outlined" size="small" @click="fetchData">
        <v-tooltip activator="parent" location="bottom">Refresh now</v-tooltip>
      </v-btn>
    </div>

    <!-- Error Alert -->
    <v-alert v-if="fetchError" type="error" variant="tonal" closable class="mb-4" @click:close="fetchError = null">
      {{ fetchError }}
    </v-alert>

    <!-- Summary Cards -->
    <v-row class="mb-4">
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="d-flex align-center mb-1">
              <v-icon color="primary" size="28">mdi-pipe</v-icon>
              <span class="text-subtitle-1 ml-2">Pipelines</span>
            </div>
            <div class="text-h3 font-weight-bold">{{ totalPipelines }}</div>
            <div class="text-caption text-medium-emphasis">
              {{ pipelinesStore.runningGroups.length }} groups running
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="d-flex align-center mb-1">
              <v-icon color="success" size="28">mdi-server-network</v-icon>
              <span class="text-subtitle-1 ml-2">Workers</span>
            </div>
            <div class="text-h3 font-weight-bold">{{ clusterStore.workers.length }}</div>
            <div class="d-flex gap-1 mt-1">
              <v-chip color="success" size="x-small" variant="flat">{{ clusterStore.healthyWorkers.length }} healthy</v-chip>
              <v-chip v-if="clusterStore.unhealthyWorkers.length > 0" color="error" size="x-small" variant="flat">
                {{ clusterStore.unhealthyWorkers.length }} unhealthy
              </v-chip>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="d-flex align-center mb-1">
              <v-icon color="info" size="28">mdi-connection</v-icon>
              <span class="text-subtitle-1 ml-2">Connectors</span>
            </div>
            <div class="text-h3 font-weight-bold">{{ healthyConnectors + unhealthyConnectors }}</div>
            <div class="d-flex gap-1 mt-1">
              <v-chip color="success" size="x-small" variant="flat">{{ healthyConnectors }} connected</v-chip>
              <v-chip v-if="unhealthyConnectors > 0" color="error" size="x-small" variant="flat">
                {{ unhealthyConnectors }} disconnected
              </v-chip>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="d-flex align-center mb-1">
              <v-icon color="warning" size="28">mdi-broadcast</v-icon>
              <span class="text-subtitle-1 ml-2">Live Stream</span>
            </div>
            <div class="text-h3 font-weight-bold">{{ liveEvents.length }}</div>
            <v-btn
              size="x-small"
              :color="eventStreamEnabled ? 'error' : 'success'"
              variant="flat"
              class="mt-1"
              @click="toggleEventStream"
            >
              {{ eventStreamEnabled ? 'Stop' : 'Start' }} Stream
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Throughput Chart -->
    <v-row class="mb-4">
      <v-col cols="12">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-chart-line</v-icon>
            Event Throughput
          </v-card-title>
          <v-card-text>
            <ThroughputChart :height="250" />
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Pipeline Status Table -->
    <v-row class="mb-4">
      <v-col cols="12">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-pipe</v-icon>
            Pipeline Status
          </v-card-title>
          <v-card-text class="pa-0">
            <v-table v-if="pipelines.length > 0" density="compact" hover>
              <thead>
                <tr>
                  <th>Pipeline</th>
                  <th>Worker</th>
                  <th class="text-right">Events In</th>
                  <th class="text-right">Events Out</th>
                  <th class="text-right">Throughput</th>
                  <th>Connectors</th>
                  <th class="text-center">Actions</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="p in pipelines" :key="p.pipeline_name + p.worker_id">
                  <td>
                    <a class="text-decoration-none cursor-pointer" @click="showPipelineDetail(p.pipeline_name)">
                      <code>{{ p.pipeline_name }}</code>
                    </a>
                  </td>
                  <td>
                    <v-chip size="x-small" variant="tonal">{{ p.worker_id }}</v-chip>
                  </td>
                  <td class="text-right">{{ p.events_in.toLocaleString() }}</td>
                  <td class="text-right">{{ p.events_out.toLocaleString() }}</td>
                  <td class="text-right">
                    <span class="font-weight-medium">
                      {{ getPipelineThroughput(p.pipeline_name, p.worker_id).toFixed(1) }}
                    </span>
                    <span class="text-caption text-medium-emphasis"> evt/s</span>
                  </td>
                  <td>
                    <div class="d-flex gap-1">
                      <v-chip
                        v-for="c in p.connector_health"
                        :key="c.connector_name"
                        :color="c.connected ? 'success' : 'error'"
                        size="x-small"
                        variant="flat"
                        :prepend-icon="connectorIcon(c.connector_type)"
                      >
                        {{ c.connector_name }}
                      </v-chip>
                      <span v-if="p.connector_health.length === 0" class="text-caption text-medium-emphasis">--</span>
                    </div>
                  </td>
                  <td class="text-center">
                    <v-btn icon="mdi-information-outline" variant="text" size="small" @click="showPipelineDetail(p.pipeline_name)" />
                  </td>
                </tr>
              </tbody>
            </v-table>
            <div v-else class="pa-8 text-center text-medium-emphasis">
              <v-icon size="48" class="mb-2">mdi-pipe-disconnected</v-icon>
              <div>No deployed pipelines</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Connector Health Panel -->
    <v-row v-if="allConnectors.length > 0" class="mb-4">
      <v-col cols="12">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-connection</v-icon>
            Connector Health
          </v-card-title>
          <v-card-text class="pa-0">
            <v-table density="compact" hover>
              <thead>
                <tr>
                  <th>Connector</th>
                  <th>Type</th>
                  <th>Pipeline</th>
                  <th>Worker</th>
                  <th class="text-center">Status</th>
                  <th class="text-right">Messages</th>
                  <th>Last Message</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="c in allConnectors" :key="c.connector_name + c.pipeline_name">
                  <td>
                    <v-icon :icon="connectorIcon(c.connector_type)" size="small" class="mr-1" />
                    <code>{{ c.connector_name }}</code>
                  </td>
                  <td>
                    <v-chip size="x-small" variant="tonal">{{ c.connector_type }}</v-chip>
                  </td>
                  <td><code>{{ c.pipeline_name }}</code></td>
                  <td><v-chip size="x-small" variant="tonal">{{ c.worker_id }}</v-chip></td>
                  <td class="text-center">
                    <v-icon :color="c.connected ? 'success' : 'error'" size="small">
                      {{ c.connected ? 'mdi-check-circle' : 'mdi-close-circle' }}
                    </v-icon>
                  </td>
                  <td class="text-right">{{ c.messages_received.toLocaleString() }}</td>
                  <td>
                    <span v-if="c.seconds_since_last_message === 0" class="text-success">just now</span>
                    <span v-else>{{ formatDuration(c.seconds_since_last_message) }}</span>
                  </td>
                  <td>
                    <span v-if="c.last_error" class="text-error text-caption">{{ c.last_error }}</span>
                    <span v-else class="text-medium-emphasis">--</span>
                  </td>
                </tr>
              </tbody>
            </v-table>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Live Event Stream -->
    <v-row>
      <v-col cols="12">
        <v-card>
          <v-card-title class="d-flex align-center">
            <v-icon class="mr-2">mdi-broadcast</v-icon>
            Live Event Stream
            <v-spacer />
            <v-btn
              v-if="eventStreamEnabled"
              :icon="eventStreamPaused ? 'mdi-play' : 'mdi-pause'"
              variant="text"
              size="small"
              class="mr-1"
              @click="eventStreamPaused = !eventStreamPaused"
            >
              <v-tooltip activator="parent" location="bottom">
                {{ eventStreamPaused ? 'Resume' : 'Pause' }}
              </v-tooltip>
            </v-btn>
            <v-btn icon="mdi-delete-outline" variant="text" size="small" class="mr-1" @click="clearEvents">
              <v-tooltip activator="parent" location="bottom">Clear</v-tooltip>
            </v-btn>
            <v-btn
              :color="eventStreamEnabled ? 'error' : 'success'"
              size="small"
              variant="flat"
              :prepend-icon="eventStreamEnabled ? 'mdi-stop' : 'mdi-play'"
              @click="toggleEventStream"
            >
              {{ eventStreamEnabled ? 'Stop' : 'Start' }}
            </v-btn>
          </v-card-title>
          <v-card-text class="pa-0">
            <div v-if="liveEvents.length > 0" style="max-height: 400px; overflow-y: auto;">
              <v-table density="compact">
                <thead>
                  <tr>
                    <th style="width: 140px">Time</th>
                    <th>Event Type</th>
                    <th>Pipeline</th>
                    <th>Data</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="e in liveEvents" :key="e.id">
                    <td class="text-caption font-weight-medium">
                      {{ e.timestamp.toLocaleTimeString() }}.{{ String(e.timestamp.getMilliseconds()).padStart(3, '0') }}
                    </td>
                    <td>
                      <v-chip size="x-small" color="primary" variant="flat">{{ e.event_type }}</v-chip>
                    </td>
                    <td>
                      <code v-if="e.pipeline_id" class="text-caption">{{ e.pipeline_id }}</code>
                      <span v-else class="text-medium-emphasis">--</span>
                    </td>
                    <td class="text-caption" style="max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                      {{ JSON.stringify(e.data) }}
                    </td>
                  </tr>
                </tbody>
              </v-table>
            </div>
            <div v-else class="pa-8 text-center text-medium-emphasis">
              <v-icon size="48" class="mb-2">mdi-broadcast-off</v-icon>
              <div v-if="!eventStreamEnabled">Click "Start" to begin streaming live events via WebSocket</div>
              <div v-else>Waiting for events...</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Pipeline Detail Dialog -->
    <v-dialog v-model="detailDialog" max-width="700">
      <v-card v-if="selectedPipelineMetrics">
        <v-card-title class="d-flex align-center">
          <v-icon class="mr-2">mdi-pipe</v-icon>
          {{ selectedPipelineMetrics.pipeline_name }}
          <v-spacer />
          <v-btn icon="mdi-close" variant="text" @click="detailDialog = false" />
        </v-card-title>
        <v-card-text>
          <!-- Pipeline metrics -->
          <v-row class="mb-4">
            <v-col cols="4">
              <div class="text-caption text-medium-emphasis">Worker</div>
              <v-chip size="small" variant="tonal">{{ selectedPipelineMetrics.worker_id }}</v-chip>
            </v-col>
            <v-col cols="4">
              <div class="text-caption text-medium-emphasis">Events In</div>
              <div class="text-h5 font-weight-bold">{{ selectedPipelineMetrics.events_in.toLocaleString() }}</div>
            </v-col>
            <v-col cols="4">
              <div class="text-caption text-medium-emphasis">Events Out</div>
              <div class="text-h5 font-weight-bold">{{ selectedPipelineMetrics.events_out.toLocaleString() }}</div>
            </v-col>
          </v-row>

          <v-row class="mb-4">
            <v-col cols="4">
              <div class="text-caption text-medium-emphasis">Throughput</div>
              <div class="text-h5 font-weight-bold">
                {{ getPipelineThroughput(selectedPipelineMetrics.pipeline_name, selectedPipelineMetrics.worker_id).toFixed(1) }}
                <span class="text-body-2 text-medium-emphasis">evt/s</span>
              </div>
            </v-col>
            <v-col cols="4">
              <div class="text-caption text-medium-emphasis">Connectors</div>
              <div class="text-h5 font-weight-bold">{{ selectedPipelineConnectors.length }}</div>
            </v-col>
            <v-col cols="4">
              <div class="text-caption text-medium-emphasis">Selectivity</div>
              <div class="text-h5 font-weight-bold">
                {{ selectedPipelineMetrics.events_in > 0
                  ? ((selectedPipelineMetrics.events_out / selectedPipelineMetrics.events_in) * 100).toFixed(1)
                  : '0.0' }}%
              </div>
            </v-col>
          </v-row>

          <!-- Connector health for this pipeline -->
          <div v-if="selectedPipelineConnectors.length > 0">
            <div class="text-subtitle-2 mb-2">Connector Health</div>
            <v-list density="compact">
              <v-list-item v-for="c in selectedPipelineConnectors" :key="c.connector_name">
                <template #prepend>
                  <v-icon :icon="connectorIcon(c.connector_type)" :color="c.connected ? 'success' : 'error'" />
                </template>
                <v-list-item-title>
                  {{ c.connector_name }}
                  <v-chip size="x-small" variant="tonal" class="ml-1">{{ c.connector_type }}</v-chip>
                </v-list-item-title>
                <v-list-item-subtitle>
                  {{ c.messages_received.toLocaleString() }} messages
                  <span v-if="c.seconds_since_last_message > 0">
                    &middot; last: {{ formatDuration(c.seconds_since_last_message) }}
                  </span>
                  <span v-if="c.last_error" class="text-error"> &middot; {{ c.last_error }}</span>
                </v-list-item-subtitle>
                <template #append>
                  <v-icon :color="c.connected ? 'success' : 'error'" size="small">
                    {{ c.connected ? 'mdi-check-circle' : 'mdi-close-circle' }}
                  </v-icon>
                </template>
              </v-list-item>
            </v-list>
          </div>
        </v-card-text>
      </v-card>
    </v-dialog>

    <!-- Loading overlay -->
    <v-overlay :model-value="loading" class="align-center justify-center" contained>
      <v-progress-circular indeterminate size="64" />
    </v-overlay>
  </div>
</template>
