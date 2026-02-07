<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'
import WorkerGrid from '@/components/cluster/WorkerGrid.vue'
import TopologyGraph from '@/components/cluster/TopologyGraph.vue'
import HealthIndicator from '@/components/cluster/HealthIndicator.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import type { Worker } from '@/types/cluster'

const clusterStore = useClusterStore()

const activeTab = ref('workers')
const selectedWorker = ref<Worker | null>(null)
const detailDrawer = ref(false)
const confirmDialog = ref(false)
const confirmAction = ref<'remove' | 'drain'>('remove')
const actionLoading = ref(false)

let pollInterval: ReturnType<typeof setInterval> | null = null

const workers = computed(() => clusterStore.workers)
const loading = computed(() => clusterStore.loading)
const error = computed(() => clusterStore.error)
const workerDetail = computed(() => clusterStore.selectedWorker)

// Filter workers
const workerFilter = ref<'all' | 'healthy' | 'unhealthy' | 'draining'>('all')
const filteredWorkers = computed(() => {
  switch (workerFilter.value) {
    case 'healthy':
      return clusterStore.healthyWorkers
    case 'unhealthy':
      return clusterStore.unhealthyWorkers
    case 'draining':
      return clusterStore.drainingWorkers
    default:
      return workers.value
  }
})

// Handle worker selection
async function handleWorkerSelect(worker: Worker): Promise<void> {
  selectedWorker.value = worker
  detailDrawer.value = true
  await clusterStore.fetchWorkerDetail(worker.id)
}

function closeDetailDrawer(): void {
  detailDrawer.value = false
  selectedWorker.value = null
  clusterStore.selectedWorker = null
}

// Worker actions
function confirmRemoveWorker(): void {
  confirmAction.value = 'remove'
  confirmDialog.value = true
}

function confirmDrainWorker(): void {
  confirmAction.value = 'drain'
  confirmDialog.value = true
}

async function executeAction(): Promise<void> {
  if (!selectedWorker.value) return

  actionLoading.value = true
  try {
    if (confirmAction.value === 'remove') {
      await clusterStore.removeWorker(selectedWorker.value.id)
    } else {
      await clusterStore.drainWorker(selectedWorker.value.id)
    }
    confirmDialog.value = false
    closeDetailDrawer()
  } catch {
    // Error is handled by store
  } finally {
    actionLoading.value = false
  }
}

async function fetchData(): Promise<void> {
  await Promise.all([
    clusterStore.fetchWorkers(),
    clusterStore.fetchTopology(),
  ])
}

onMounted(() => {
  fetchData()
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
      <h1 class="text-h4">Cluster Management</h1>
      <v-spacer />
      <HealthIndicator />
    </div>

    <!-- Error Alert -->
    <v-alert
      v-if="error"
      type="error"
      closable
      class="mb-4"
      @click:close="clusterStore.clearError"
    >
      {{ error }}
    </v-alert>

    <!-- Tabs -->
    <v-tabs v-model="activeTab" class="mb-4">
      <v-tab value="workers">
        <v-icon start>mdi-server</v-icon>
        Workers
        <v-badge
          :content="workers.length"
          color="primary"
          inline
          class="ml-2"
        />
      </v-tab>
      <v-tab value="topology">
        <v-icon start>mdi-graph</v-icon>
        Topology
      </v-tab>
    </v-tabs>

    <v-window v-model="activeTab">
      <!-- Workers Tab -->
      <v-window-item value="workers">
        <!-- Filter Chips -->
        <div class="d-flex gap-2 mb-4">
          <v-chip
            :variant="workerFilter === 'all' ? 'flat' : 'outlined'"
            :color="workerFilter === 'all' ? 'primary' : undefined"
            @click="workerFilter = 'all'"
          >
            All ({{ workers.length }})
          </v-chip>
          <v-chip
            :variant="workerFilter === 'healthy' ? 'flat' : 'outlined'"
            :color="workerFilter === 'healthy' ? 'success' : undefined"
            @click="workerFilter = 'healthy'"
          >
            Healthy ({{ clusterStore.healthyWorkers.length }})
          </v-chip>
          <v-chip
            :variant="workerFilter === 'unhealthy' ? 'flat' : 'outlined'"
            :color="workerFilter === 'unhealthy' ? 'error' : undefined"
            @click="workerFilter = 'unhealthy'"
          >
            Unhealthy ({{ clusterStore.unhealthyWorkers.length }})
          </v-chip>
          <v-chip
            :variant="workerFilter === 'draining' ? 'flat' : 'outlined'"
            :color="workerFilter === 'draining' ? 'warning' : undefined"
            @click="workerFilter = 'draining'"
          >
            Draining ({{ clusterStore.drainingWorkers.length }})
          </v-chip>
        </div>

        <WorkerGrid
          :workers="filteredWorkers"
          :loading="loading"
          @select="handleWorkerSelect"
        />
      </v-window-item>

      <!-- Topology Tab -->
      <v-window-item value="topology">
        <v-card>
          <v-card-text class="pa-0">
            <TopologyGraph
              :topology="clusterStore.topology"
              :loading="loading"
              @select-worker="handleWorkerSelect"
            />
          </v-card-text>
        </v-card>
      </v-window-item>
    </v-window>

    <!-- Worker Detail Drawer -->
    <v-navigation-drawer
      v-model="detailDrawer"
      location="right"
      temporary
      width="400"
    >
      <template v-if="selectedWorker">
        <v-toolbar flat density="compact">
          <v-toolbar-title>Worker Details</v-toolbar-title>
          <v-spacer />
          <v-btn icon @click="closeDetailDrawer">
            <v-icon>mdi-close</v-icon>
          </v-btn>
        </v-toolbar>

        <v-card-text v-if="workerDetail">
          <v-list density="compact">
            <v-list-item>
              <v-list-item-title>ID</v-list-item-title>
              <v-list-item-subtitle class="text-wrap font-monospace">
                {{ workerDetail.id }}
              </v-list-item-subtitle>
            </v-list-item>

            <v-list-item>
              <v-list-item-title>Address</v-list-item-title>
              <v-list-item-subtitle class="font-monospace">
                {{ workerDetail.address }}
              </v-list-item-subtitle>
            </v-list-item>

            <v-list-item>
              <v-list-item-title>Status</v-list-item-title>
              <v-list-item-subtitle>
                <StatusChip :status="workerDetail.status" />
              </v-list-item-subtitle>
            </v-list-item>

            <v-list-item>
              <v-list-item-title>Pipelines</v-list-item-title>
              <v-list-item-subtitle>
                {{ workerDetail.pipelines_running }} / {{ workerDetail.max_pipelines }}
              </v-list-item-subtitle>
            </v-list-item>

            <v-list-item v-if="workerDetail.last_heartbeat">
              <v-list-item-title>Last Heartbeat</v-list-item-title>
              <v-list-item-subtitle>
                {{ new Date(workerDetail.last_heartbeat).toLocaleString() }}
              </v-list-item-subtitle>
            </v-list-item>

            <v-list-item v-if="workerDetail.registered_at">
              <v-list-item-title>Registered</v-list-item-title>
              <v-list-item-subtitle>
                {{ new Date(workerDetail.registered_at).toLocaleString() }}
              </v-list-item-subtitle>
            </v-list-item>
          </v-list>

          <v-divider class="my-4" />

          <!-- Pipelines on this worker -->
          <div v-if="workerDetail.pipelines && workerDetail.pipelines.length > 0">
            <div class="text-subtitle-2 mb-2">Running Pipelines</div>
            <v-list density="compact">
              <v-list-item
                v-for="pipeline in workerDetail.pipelines"
                :key="pipeline.pipeline_id"
              >
                <v-list-item-title>{{ pipeline.pipeline_name }}</v-list-item-title>
                <v-list-item-subtitle>
                  Group: {{ pipeline.group_id }}
                </v-list-item-subtitle>
                <template #append>
                  <StatusChip :status="pipeline.status" size="x-small" />
                </template>
              </v-list-item>
            </v-list>
          </div>
          <div v-else class="text-medium-emphasis text-center py-4">
            No pipelines running on this worker
          </div>

          <v-divider class="my-4" />

          <!-- Actions -->
          <div class="d-flex gap-2">
            <v-btn
              v-if="workerDetail.status === 'ready'"
              color="warning"
              variant="outlined"
              prepend-icon="mdi-progress-clock"
              @click="confirmDrainWorker"
            >
              Drain
            </v-btn>
            <v-btn
              color="error"
              variant="outlined"
              prepend-icon="mdi-delete"
              @click="confirmRemoveWorker"
            >
              Remove
            </v-btn>
          </div>
        </v-card-text>

        <v-card-text v-else class="d-flex justify-center">
          <v-progress-circular indeterminate />
        </v-card-text>
      </template>
    </v-navigation-drawer>

    <!-- Confirm Dialog -->
    <ConfirmDialog
      v-model="confirmDialog"
      :title="confirmAction === 'remove' ? 'Remove Worker' : 'Drain Worker'"
      :message="confirmAction === 'remove'
        ? 'Are you sure you want to remove this worker from the cluster? This will terminate all pipelines running on it.'
        : 'Are you sure you want to drain this worker? It will stop accepting new pipelines and existing pipelines will be migrated.'"
      :confirm-text="confirmAction === 'remove' ? 'Remove' : 'Drain'"
      :confirm-color="confirmAction === 'remove' ? 'error' : 'warning'"
      :loading="actionLoading"
      @confirm="executeAction"
    />
  </div>
</template>

<script lang="ts">
import StatusChip from '@/components/common/StatusChip.vue'
</script>
