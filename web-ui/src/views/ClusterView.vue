<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'
import WorkerGrid from '@/components/cluster/WorkerGrid.vue'
import TopologyGraph from '@/components/cluster/TopologyGraph.vue'
import HealthIndicator from '@/components/cluster/HealthIndicator.vue'
import ClusterHealthPanel from '@/components/cluster/ClusterHealthPanel.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import type { Worker, PipelinePlacement } from '@/types/cluster'

const clusterStore = useClusterStore()

const activeTab = ref('health')
const selectedWorker = ref<Worker | null>(null)
const detailDrawer = ref(false)
const confirmDialog = ref(false)
const confirmAction = ref<'remove' | 'drain'>('remove')
const actionLoading = ref(false)

// Migration dialog state
const migrateDialog = ref(false)
const migratePipeline = ref<PipelinePlacement | null>(null)
const migrateTargetWorker = ref('')
const migrateLoading = ref(false)

// Rebalance state
const rebalanceLoading = ref(false)

// Snackbar state
const snackbar = ref(false)
const snackbarText = ref('')
const snackbarColor = ref('success')

let pollInterval: ReturnType<typeof setInterval> | null = null

const workers = computed(() => clusterStore.workers)
const loading = computed(() => clusterStore.loading)
const error = computed(() => clusterStore.error)
const workerDetail = computed(() => clusterStore.selectedWorker)
const migrations = computed(() => clusterStore.migrations)
const clusterHealth = computed(() => clusterStore.clusterHealth)

// Available target workers for migration (healthy, not the current worker)
const migrateTargetWorkers = computed(() =>
  clusterStore.healthyWorkers.filter((w) => w.id !== selectedWorker.value?.id)
)

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

// Format duration
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

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

// Rebalance
async function handleRebalance(): Promise<void> {
  rebalanceLoading.value = true
  try {
    const result = await clusterStore.rebalance()
    snackbarText.value = `Rebalance triggered \u2014 ${result.migrations_started} migration${result.migrations_started !== 1 ? 's' : ''} started`
    snackbarColor.value = 'success'
    snackbar.value = true
    if (result.migrations_started > 0) {
      activeTab.value = 'migrations'
    }
  } catch {
    snackbarText.value = 'Failed to trigger rebalance'
    snackbarColor.value = 'error'
    snackbar.value = true
  } finally {
    rebalanceLoading.value = false
  }
}

// Migrate pipeline
function openMigrateDialog(pipeline: PipelinePlacement): void {
  migratePipeline.value = pipeline
  migrateTargetWorker.value = ''
  migrateDialog.value = true
}

async function executeMigrate(): Promise<void> {
  if (!migratePipeline.value || !migrateTargetWorker.value) return

  migrateLoading.value = true
  try {
    await clusterStore.migratePipeline(
      migratePipeline.value.group_id,
      migratePipeline.value.pipeline_name,
      migrateTargetWorker.value,
    )
    migrateDialog.value = false
    snackbarText.value = `Migration started for ${migratePipeline.value.pipeline_name}`
    snackbarColor.value = 'success'
    snackbar.value = true
    activeTab.value = 'migrations'
  } catch {
    snackbarText.value = 'Failed to migrate pipeline'
    snackbarColor.value = 'error'
    snackbar.value = true
  } finally {
    migrateLoading.value = false
  }
}

async function fetchData(): Promise<void> {
  await Promise.all([
    clusterStore.fetchWorkers(),
    clusterStore.fetchTopology(),
    clusterStore.fetchMigrations(),
    clusterStore.fetchClusterHealth(),
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
      <v-btn
        class="mr-2"
        variant="outlined"
        prepend-icon="mdi-scale-balance"
        :loading="rebalanceLoading"
        @click="handleRebalance"
      >
        Rebalance
      </v-btn>
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
      <v-tab value="health">
        <v-icon start>mdi-heart-pulse</v-icon>
        Health
      </v-tab>
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
      <v-tab value="migrations">
        <v-icon start>mdi-swap-horizontal</v-icon>
        Migrations
        <v-badge
          v-if="clusterStore.activeMigrations.length > 0"
          :content="clusterStore.activeMigrations.length"
          color="warning"
          inline
          class="ml-2"
        />
      </v-tab>
    </v-tabs>

    <v-window v-model="activeTab">
      <!-- Health Tab -->
      <v-window-item value="health">
        <ClusterHealthPanel :health="clusterHealth" />
      </v-window-item>

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

      <!-- Migrations Tab -->
      <v-window-item value="migrations">
        <v-card>
          <v-card-text v-if="migrations.length === 0" class="text-center text-medium-emphasis py-8">
            <v-icon size="48" class="mb-2">mdi-swap-horizontal</v-icon>
            <div>No active migrations</div>
          </v-card-text>
          <v-table v-else density="compact">
            <thead>
              <tr>
                <th>Pipeline</th>
                <th>Group</th>
                <th>Source Worker</th>
                <th>Target Worker</th>
                <th>Status</th>
                <th>Reason</th>
                <th class="text-right">Duration</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="m in migrations" :key="m.id">
                <td><code>{{ m.pipeline_name }}</code></td>
                <td>
                  <v-chip size="x-small" variant="tonal">{{ m.group_id }}</v-chip>
                </td>
                <td class="font-monospace text-caption">{{ m.source_worker }}</td>
                <td class="font-monospace text-caption">{{ m.target_worker }}</td>
                <td><StatusChip :status="m.status" size="x-small" /></td>
                <td><StatusChip :status="m.reason" size="x-small" /></td>
                <td class="text-right">{{ formatDuration(m.elapsed_ms) }}</td>
              </tr>
            </tbody>
          </v-table>
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
                  <v-btn
                    size="x-small"
                    variant="text"
                    icon="mdi-swap-horizontal"
                    color="primary"
                    title="Migrate Pipeline"
                    class="mr-1"
                    @click.stop="openMigrateDialog(pipeline)"
                  />
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

    <!-- Migrate Pipeline Dialog -->
    <v-dialog v-model="migrateDialog" max-width="480">
      <v-card>
        <v-card-title>Migrate Pipeline</v-card-title>
        <v-card-text>
          <p class="mb-4">
            Migrate <strong>{{ migratePipeline?.pipeline_name }}</strong> to another worker.
            State will be checkpointed and restored on the target.
          </p>
          <v-select
            v-model="migrateTargetWorker"
            :items="migrateTargetWorkers"
            item-title="address"
            item-value="id"
            label="Target Worker"
            variant="outlined"
            :disabled="migrateLoading"
          >
            <template #item="{ item, props: itemProps }">
              <v-list-item v-bind="itemProps">
                <v-list-item-subtitle>
                  {{ item.raw.pipelines_running }} / {{ item.raw.max_pipelines }} pipelines
                </v-list-item-subtitle>
              </v-list-item>
            </template>
          </v-select>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="migrateDialog = false">Cancel</v-btn>
          <v-btn
            color="primary"
            variant="flat"
            :loading="migrateLoading"
            :disabled="!migrateTargetWorker"
            @click="executeMigrate"
          >
            Migrate
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Snackbar -->
    <v-snackbar v-model="snackbar" :color="snackbarColor" :timeout="4000">
      {{ snackbarText }}
    </v-snackbar>
  </div>
</template>

<script lang="ts">
import StatusChip from '@/components/common/StatusChip.vue'
</script>
