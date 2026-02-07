<script setup lang="ts">
import { computed } from 'vue'
import { useMetricsStore } from '@/stores/metrics'
import { useClusterStore } from '@/stores/cluster'

const metricsStore = useMetricsStore()
const clusterStore = useClusterStore()

const workerMetrics = computed(() => metricsStore.workerMetrics)
const workers = computed(() => clusterStore.workers)

// Map worker metrics to workers for display
const workerData = computed(() => {
  return workers.value.map((worker) => {
    const metrics = workerMetrics.value.find((m) => m.worker_id === worker.id)
    return {
      id: worker.id,
      address: worker.address,
      status: worker.status,
      pipelineCount: worker.pipelines_running,
      eventsProcessed: metrics?.events_processed || 0,
      throughput: metrics?.throughput_eps || 0,
      latency: metrics?.avg_latency_ms || 0,
      errors: metrics?.errors || 0,
    }
  })
})

function getStatusColor(status: string): string {
  switch (status) {
    case 'ready':
      return 'success'
    case 'unhealthy':
      return 'error'
    case 'draining':
      return 'warning'
    default:
      return 'grey'
  }
}

function formatNumber(num: number): string {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`
  }
  return num.toString()
}
</script>

<template>
  <v-card>
    <v-card-title>
      <v-icon class="mr-2">mdi-server-network</v-icon>
      Worker Metrics
      <v-badge
        v-if="workerData.length > 0"
        :content="workerData.length"
        color="primary"
        inline
        class="ml-2"
      />
    </v-card-title>

    <v-card-text class="pa-0">
      <!-- Empty State -->
      <div v-if="workerData.length === 0" class="text-center pa-8 text-medium-emphasis">
        <v-icon size="48" class="mb-2">mdi-server-off</v-icon>
        <div>No workers connected</div>
      </div>

      <!-- Workers Table -->
      <v-table v-else density="comfortable">
        <thead>
          <tr>
            <th class="text-left">Worker</th>
            <th class="text-left">Status</th>
            <th class="text-right">Pipelines</th>
            <th class="text-right">Throughput</th>
            <th class="text-right">Latency</th>
            <th class="text-right">Events</th>
            <th class="text-right">Errors</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="worker in workerData" :key="worker.id">
            <td>
              <div class="d-flex align-center">
                <v-avatar :color="getStatusColor(worker.status)" size="32" class="mr-2">
                  <v-icon size="16">mdi-server</v-icon>
                </v-avatar>
                <div>
                  <div class="text-body-2 font-weight-medium">
                    {{ worker.id.substring(0, 8) }}...
                  </div>
                  <div class="text-caption text-medium-emphasis font-monospace">
                    {{ worker.address }}
                  </div>
                </div>
              </div>
            </td>
            <td>
              <v-chip :color="getStatusColor(worker.status)" size="small" variant="flat">
                {{ worker.status }}
              </v-chip>
            </td>
            <td class="text-right">
              <v-chip size="small" variant="outlined">
                {{ worker.pipelineCount }}
              </v-chip>
            </td>
            <td class="text-right">
              <span class="font-weight-medium">{{ worker.throughput.toFixed(1) }}</span>
              <span class="text-caption text-medium-emphasis"> evt/s</span>
            </td>
            <td class="text-right">
              <span class="font-weight-medium">{{ worker.latency.toFixed(2) }}</span>
              <span class="text-caption text-medium-emphasis"> ms</span>
            </td>
            <td class="text-right">
              {{ formatNumber(worker.eventsProcessed) }}
            </td>
            <td class="text-right">
              <span :class="{ 'text-error': worker.errors > 0 }">
                {{ formatNumber(worker.errors) }}
              </span>
            </td>
          </tr>
        </tbody>
      </v-table>
    </v-card-text>
  </v-card>
</template>
