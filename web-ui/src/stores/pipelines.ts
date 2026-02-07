import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type {
  PipelineGroup,
  PipelineGroupSpec,
  PipelineInfo,
  PipelineStatus,
  DeployFormState,
  PipelineFormEntry,
  RouteFormEntry,
} from '@/types/pipeline'
import * as clusterApi from '@/api/cluster'

// Default form state
function createDefaultFormState(): DeployFormState {
  return {
    name: '',
    pipelines: [createDefaultPipelineEntry()],
    routes: [],
  }
}

function createDefaultPipelineEntry(): PipelineFormEntry {
  return {
    name: '',
    vpl_source: '',
    affinityType: 'any',
    affinityValue: '',
  }
}

function createDefaultRouteEntry(): RouteFormEntry {
  return {
    source: '',
    target: '',
    eventTypes: '',
    filter: '',
  }
}

export const usePipelinesStore = defineStore('pipelines', () => {
  // State
  const groups = ref<PipelineGroup[]>([])
  const selectedGroup = ref<PipelineGroup | null>(null)
  const deployForm = ref<DeployFormState>(createDefaultFormState())
  const loading = ref(false)
  const deploying = ref(false)
  const error = ref<string | null>(null)
  const deployDialogOpen = ref(false)

  // Computed (status values match Rust GroupStatus snake_case)
  const runningGroups = computed(() =>
    groups.value.filter((g) => g.status === 'running')
  )

  const failedGroups = computed(() =>
    groups.value.filter((g) => g.status === 'failed' || g.status === 'partially_running')
  )

  const pendingGroups = computed(() =>
    groups.value.filter((g) => g.status === 'deploying')
  )

  const allPipelines = computed(() => {
    const pipelines: (PipelineInfo & { groupId: string; groupName: string })[] = []
    for (const group of groups.value) {
      for (const placement of group.placements || []) {
        pipelines.push({
          id: placement.pipeline_id,
          name: placement.pipeline_name,
          worker_id: placement.worker_id,
          worker_address: placement.worker_address,
          status: placement.status as PipelineStatus,
          groupId: group.id,
          groupName: group.name,
        })
      }
    }
    return pipelines
  })

  const pipelinesByWorker = computed(() => {
    const byWorker: Record<string, PipelineInfo[]> = {}
    for (const pipeline of allPipelines.value) {
      if (!byWorker[pipeline.worker_id]) {
        byWorker[pipeline.worker_id] = []
      }
      byWorker[pipeline.worker_id].push(pipeline)
    }
    return byWorker
  })

  // Actions
  async function fetchGroups(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      groups.value = await clusterApi.listPipelineGroups()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch pipeline groups'
    } finally {
      loading.value = false
    }
  }

  async function fetchGroupDetail(id: string): Promise<void> {
    loading.value = true
    error.value = null
    try {
      selectedGroup.value = await clusterApi.getPipelineGroup(id)
      // Update in groups array too
      const index = groups.value.findIndex((g) => g.id === id)
      if (index !== -1) {
        groups.value[index] = selectedGroup.value
      }
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch pipeline group'
    } finally {
      loading.value = false
    }
  }

  async function deploy(): Promise<PipelineGroup | null> {
    deploying.value = true
    error.value = null

    try {
      // Transform form state to API spec (matching Rust PipelineGroupSpec)
      const spec: PipelineGroupSpec = {
        name: deployForm.value.name,
        pipelines: deployForm.value.pipelines.map((p) => ({
          name: p.name,
          source: p.vpl_source,
          worker_affinity: p.affinityType === 'any' ? undefined : p.affinityValue || undefined,
        })),
        routes: deployForm.value.routes.length > 0 ? deployForm.value.routes.map((r) => ({
          from_pipeline: r.source,
          to_pipeline: r.target,
          event_types: r.eventTypes ? r.eventTypes.split(',').map((t) => t.trim()) : ['*'],
        })) : undefined,
      }

      const group = await clusterApi.deployPipelineGroup(spec)
      groups.value.push(group)
      resetForm()
      deployDialogOpen.value = false
      return group
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to deploy pipeline group'
      return null
    } finally {
      deploying.value = false
    }
  }

  async function teardown(id: string): Promise<void> {
    try {
      await clusterApi.teardownGroup(id)
      groups.value = groups.value.filter((g) => g.id !== id)
      if (selectedGroup.value?.id === id) {
        selectedGroup.value = null
      }
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to teardown pipeline group'
      throw e
    }
  }

  async function reload(id: string): Promise<void> {
    try {
      await clusterApi.reloadPipelineGroup(id)
      // Refresh the group
      await fetchGroupDetail(id)
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to reload pipeline group'
      throw e
    }
  }

  function openDeployDialog(): void {
    resetForm()
    deployDialogOpen.value = true
  }

  function closeDeployDialog(): void {
    deployDialogOpen.value = false
  }

  function resetForm(): void {
    deployForm.value = createDefaultFormState()
  }

  function addPipeline(): void {
    deployForm.value.pipelines.push(createDefaultPipelineEntry())
  }

  function removePipeline(index: number): void {
    if (deployForm.value.pipelines.length > 1) {
      deployForm.value.pipelines.splice(index, 1)
    }
  }

  function addRoute(): void {
    deployForm.value.routes.push(createDefaultRouteEntry())
  }

  function removeRoute(index: number): void {
    deployForm.value.routes.splice(index, 1)
  }

  function selectGroup(group: PipelineGroup | null): void {
    selectedGroup.value = group
  }

  function clearError(): void {
    error.value = null
  }

  return {
    // State
    groups,
    selectedGroup,
    deployForm,
    loading,
    deploying,
    error,
    deployDialogOpen,

    // Computed
    runningGroups,
    failedGroups,
    pendingGroups,
    allPipelines,
    pipelinesByWorker,

    // Actions
    fetchGroups,
    fetchGroupDetail,
    deploy,
    teardown,
    reload,
    openDeployDialog,
    closeDeployDialog,
    resetForm,
    addPipeline,
    removePipeline,
    addRoute,
    removeRoute,
    selectGroup,
    clearError,
  }
})
