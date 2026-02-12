import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type {
  Worker,
  WorkerDetail,
  TopologyInfo,
  ClusterSummary,
  ClusterHealthMetrics,
  Alert,
  Migration,
} from '@/types/cluster'
import type { PipelineGroup, PipelineGroupSpec, EventPayload, InjectResponse, InjectBatchResponse } from '@/types/pipeline'
import * as clusterApi from '@/api/cluster'

export const useClusterStore = defineStore('cluster', () => {
  // State
  const workers = ref<Worker[]>([])
  const selectedWorker = ref<WorkerDetail | null>(null)
  const pipelineGroups = ref<PipelineGroup[]>([])
  const selectedGroup = ref<PipelineGroup | null>(null)
  const topology = ref<TopologyInfo | null>(null)
  const summary = ref<ClusterSummary | null>(null)
  const migrations = ref<Migration[]>([])
  const clusterHealth = ref<ClusterHealthMetrics | null>(null)
  const alerts = ref<Alert[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)

  // Computed
  const healthyWorkers = computed(() =>
    workers.value.filter((w) => w.status === 'ready')
  )

  const unhealthyWorkers = computed(() =>
    workers.value.filter((w) => w.status === 'unhealthy')
  )

  const drainingWorkers = computed(() =>
    workers.value.filter((w) => w.status === 'draining')
  )

  const activeGroups = computed(() =>
    pipelineGroups.value.filter((g) => g.status === 'running')
  )

  const activeMigrations = computed(() =>
    migrations.value.filter((m) => m.status !== 'completed' && m.status !== 'failed')
  )

  const recentAlerts = computed(() =>
    [...alerts.value]
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
      .slice(0, 10)
  )

  // Actions
  async function fetchWorkers(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      workers.value = await clusterApi.listWorkers()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch workers'
      console.error('Failed to fetch workers:', e)
    } finally {
      loading.value = false
    }
  }

  async function fetchWorkerDetail(id: string): Promise<void> {
    loading.value = true
    error.value = null
    try {
      selectedWorker.value = await clusterApi.getWorker(id)
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch worker details'
      console.error('Failed to fetch worker details:', e)
    } finally {
      loading.value = false
    }
  }

  async function removeWorker(id: string): Promise<void> {
    try {
      await clusterApi.deleteWorker(id)
      workers.value = workers.value.filter((w) => w.id !== id)
      if (selectedWorker.value?.id === id) {
        selectedWorker.value = null
      }
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to remove worker'
      throw e
    }
  }

  async function drainWorker(id: string, timeoutSecs?: number): Promise<void> {
    try {
      await clusterApi.drainWorker(id, timeoutSecs)
      const worker = workers.value.find((w) => w.id === id)
      if (worker) {
        worker.status = 'draining'
      }
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to drain worker'
      throw e
    }
  }

  async function fetchPipelineGroups(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      pipelineGroups.value = await clusterApi.listPipelineGroups()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch pipeline groups'
      console.error('Failed to fetch pipeline groups:', e)
    } finally {
      loading.value = false
    }
  }

  async function fetchPipelineGroupDetail(id: string): Promise<void> {
    loading.value = true
    error.value = null
    try {
      selectedGroup.value = await clusterApi.getPipelineGroup(id)
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch pipeline group'
      console.error('Failed to fetch pipeline group:', e)
    } finally {
      loading.value = false
    }
  }

  async function deployGroup(spec: PipelineGroupSpec): Promise<PipelineGroup> {
    loading.value = true
    error.value = null
    try {
      const group = await clusterApi.deployPipelineGroup(spec)
      pipelineGroups.value.push(group)
      return group
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to deploy pipeline group'
      throw e
    } finally {
      loading.value = false
    }
  }

  async function teardownGroup(id: string): Promise<void> {
    try {
      await clusterApi.teardownGroup(id)
      pipelineGroups.value = pipelineGroups.value.filter((g) => g.id !== id)
      if (selectedGroup.value?.id === id) {
        selectedGroup.value = null
      }
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to teardown group'
      throw e
    }
  }

  async function injectEvent(groupId: string, event: EventPayload): Promise<InjectResponse> {
    try {
      return await clusterApi.injectEvent(groupId, event)
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to inject event'
      throw e
    }
  }

  async function injectBatch(groupId: string, eventsText: string): Promise<InjectBatchResponse> {
    try {
      return await clusterApi.injectBatch(groupId, eventsText)
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to inject batch'
      throw e
    }
  }

  async function fetchTopology(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      topology.value = await clusterApi.getTopology()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch topology'
      console.error('Failed to fetch topology:', e)
    } finally {
      loading.value = false
    }
  }

  async function fetchSummary(): Promise<void> {
    try {
      summary.value = await clusterApi.getClusterSummary()
    } catch (e) {
      // Summary endpoint might not exist, compute from workers
      if (workers.value.length > 0) {
        summary.value = {
          total_workers: workers.value.length,
          healthy_workers: healthyWorkers.value.length,
          unhealthy_workers: unhealthyWorkers.value.length,
          draining_workers: drainingWorkers.value.length,
          total_pipeline_groups: pipelineGroups.value.length,
          active_pipeline_groups: activeGroups.value.length,
          total_events_processed: 0,
          events_per_second: 0,
        }
      }
    }
  }

  async function fetchClusterHealth(): Promise<void> {
    try {
      clusterHealth.value = await clusterApi.fetchClusterHealth()
    } catch {
      // Prometheus endpoint may not exist on older coordinators
    }
  }

  async function fetchMigrations(): Promise<void> {
    try {
      migrations.value = await clusterApi.listMigrations()
    } catch {
      // Migrations endpoint may not exist on older coordinators
    }
  }

  async function rebalance(): Promise<{ migrations_started: number; migration_ids: string[] }> {
    try {
      const result = await clusterApi.triggerRebalance()
      await fetchMigrations()
      return result
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to trigger rebalance'
      throw e
    }
  }

  async function migratePipeline(groupId: string, pipelineName: string, targetWorkerId: string): Promise<{ migration_id: string }> {
    try {
      const result = await clusterApi.migratePipeline(groupId, pipelineName, targetWorkerId)
      await fetchMigrations()
      return result
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to migrate pipeline'
      throw e
    }
  }

  function addAlert(alert: Alert): void {
    alerts.value.push(alert)
  }

  function acknowledgeAlert(id: string): void {
    const alert = alerts.value.find((a) => a.id === id)
    if (alert) {
      alert.acknowledged = true
    }
  }

  function clearAlerts(): void {
    alerts.value = []
  }

  function clearError(): void {
    error.value = null
  }

  return {
    // State
    workers,
    selectedWorker,
    pipelineGroups,
    selectedGroup,
    topology,
    summary,
    clusterHealth,
    migrations,
    alerts,
    loading,
    error,

    // Computed
    healthyWorkers,
    unhealthyWorkers,
    drainingWorkers,
    activeGroups,
    activeMigrations,
    recentAlerts,

    // Actions
    fetchWorkers,
    fetchWorkerDetail,
    removeWorker,
    drainWorker,
    fetchPipelineGroups,
    fetchPipelineGroupDetail,
    deployGroup,
    teardownGroup,
    injectEvent,
    injectBatch,
    fetchTopology,
    fetchSummary,
    fetchClusterHealth,
    fetchMigrations,
    rebalance,
    migratePipeline,
    addAlert,
    acknowledgeAlert,
    clearAlerts,
    clearError,
  }
})
