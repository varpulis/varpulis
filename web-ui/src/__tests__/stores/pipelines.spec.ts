import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { usePipelinesStore } from '@/stores/pipelines'
import type { PipelineGroup } from '@/types/pipeline'

describe('usePipelinesStore', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  // --- Initial State ---

  it('has empty initial state', () => {
    const store = usePipelinesStore()
    expect(store.groups).toEqual([])
    expect(store.selectedGroup).toBeNull()
    expect(store.loading).toBe(false)
    expect(store.deploying).toBe(false)
    expect(store.error).toBeNull()
    expect(store.deployDialogOpen).toBe(false)
  })

  it('has default deploy form with one empty pipeline and no routes', () => {
    const store = usePipelinesStore()
    expect(store.deployForm.name).toBe('')
    expect(store.deployForm.pipelines).toHaveLength(1)
    expect(store.deployForm.pipelines[0]).toEqual({
      name: '',
      vpl_source: '',
      affinityType: 'any',
      affinityValue: '',
    })
    expect(store.deployForm.routes).toEqual([])
  })

  // --- Computed: runningGroups ---

  it('runningGroups filters groups with running status', () => {
    const store = usePipelinesStore()
    store.groups = [
      makeGroup('g1', 'running'),
      makeGroup('g2', 'failed'),
      makeGroup('g3', 'running'),
      makeGroup('g4', 'deploying'),
    ]
    expect(store.runningGroups).toHaveLength(2)
    expect(store.runningGroups.map((g) => g.id)).toEqual(['g1', 'g3'])
  })

  // --- Computed: failedGroups ---

  it('failedGroups includes failed and partially_running groups', () => {
    const store = usePipelinesStore()
    store.groups = [
      makeGroup('g1', 'running'),
      makeGroup('g2', 'failed'),
      makeGroup('g3', 'partially_running'),
      makeGroup('g4', 'deploying'),
    ]
    expect(store.failedGroups).toHaveLength(2)
    expect(store.failedGroups.map((g) => g.id)).toEqual(['g2', 'g3'])
  })

  // --- Computed: pendingGroups ---

  it('pendingGroups filters groups with deploying status', () => {
    const store = usePipelinesStore()
    store.groups = [
      makeGroup('g1', 'running'),
      makeGroup('g2', 'deploying'),
      makeGroup('g3', 'deploying'),
    ]
    expect(store.pendingGroups).toHaveLength(2)
    expect(store.pendingGroups.map((g) => g.id)).toEqual(['g2', 'g3'])
  })

  // --- Computed: allPipelines ---

  it('allPipelines flattens placements across all groups', () => {
    const store = usePipelinesStore()
    store.groups = [
      {
        id: 'g1',
        name: 'group-1',
        status: 'running',
        pipeline_count: 2,
        placements: [
          { pipeline_name: 'p1', worker_id: 'w1', worker_address: 'http://w1', pipeline_id: 'pid1', status: 'Running' },
          { pipeline_name: 'p2', worker_id: 'w2', worker_address: 'http://w2', pipeline_id: 'pid2', status: 'Running' },
        ],
      },
      {
        id: 'g2',
        name: 'group-2',
        status: 'running',
        pipeline_count: 1,
        placements: [
          { pipeline_name: 'p3', worker_id: 'w1', worker_address: 'http://w1', pipeline_id: 'pid3', status: 'Failed' },
        ],
      },
    ]
    expect(store.allPipelines).toHaveLength(3)
    expect(store.allPipelines[0].name).toBe('p1')
    expect(store.allPipelines[0].groupId).toBe('g1')
    expect(store.allPipelines[0].groupName).toBe('group-1')
    expect(store.allPipelines[2].name).toBe('p3')
    expect(store.allPipelines[2].groupId).toBe('g2')
  })

  it('allPipelines returns empty when no groups', () => {
    const store = usePipelinesStore()
    expect(store.allPipelines).toEqual([])
  })

  it('allPipelines handles groups with no placements', () => {
    const store = usePipelinesStore()
    store.groups = [
      { id: 'g1', name: 'group-1', status: 'deploying', pipeline_count: 0, placements: [] },
    ]
    expect(store.allPipelines).toEqual([])
  })

  // --- Computed: pipelinesByWorker ---

  it('pipelinesByWorker groups pipelines by worker_id', () => {
    const store = usePipelinesStore()
    store.groups = [
      {
        id: 'g1',
        name: 'group-1',
        status: 'running',
        pipeline_count: 3,
        placements: [
          { pipeline_name: 'p1', worker_id: 'w1', worker_address: 'http://w1', pipeline_id: 'pid1', status: 'Running' },
          { pipeline_name: 'p2', worker_id: 'w2', worker_address: 'http://w2', pipeline_id: 'pid2', status: 'Running' },
          { pipeline_name: 'p3', worker_id: 'w1', worker_address: 'http://w1', pipeline_id: 'pid3', status: 'Running' },
        ],
      },
    ]
    const byWorker = store.pipelinesByWorker
    expect(Object.keys(byWorker)).toEqual(['w1', 'w2'])
    expect(byWorker['w1']).toHaveLength(2)
    expect(byWorker['w2']).toHaveLength(1)
  })

  // --- Action: resetForm ---

  it('resetForm restores the deploy form to defaults', () => {
    const store = usePipelinesStore()
    store.deployForm.name = 'my-group'
    store.deployForm.pipelines[0].name = 'my-pipeline'
    store.deployForm.pipelines[0].vpl_source = 'stream X = Y'
    store.addRoute()

    store.resetForm()

    expect(store.deployForm.name).toBe('')
    expect(store.deployForm.pipelines).toHaveLength(1)
    expect(store.deployForm.pipelines[0].name).toBe('')
    expect(store.deployForm.pipelines[0].vpl_source).toBe('')
    expect(store.deployForm.pipelines[0].affinityType).toBe('any')
    expect(store.deployForm.routes).toEqual([])
  })

  // --- Action: addPipeline ---

  it('addPipeline appends a new default pipeline entry', () => {
    const store = usePipelinesStore()
    expect(store.deployForm.pipelines).toHaveLength(1)
    store.addPipeline()
    expect(store.deployForm.pipelines).toHaveLength(2)
    expect(store.deployForm.pipelines[1]).toEqual({
      name: '',
      vpl_source: '',
      affinityType: 'any',
      affinityValue: '',
    })
  })

  // --- Action: removePipeline ---

  it('removePipeline removes pipeline at the given index', () => {
    const store = usePipelinesStore()
    store.addPipeline()
    store.deployForm.pipelines[0].name = 'first'
    store.deployForm.pipelines[1].name = 'second'
    store.removePipeline(0)
    expect(store.deployForm.pipelines).toHaveLength(1)
    expect(store.deployForm.pipelines[0].name).toBe('second')
  })

  it('removePipeline does not remove the last pipeline', () => {
    const store = usePipelinesStore()
    store.deployForm.pipelines[0].name = 'only-one'
    store.removePipeline(0)
    expect(store.deployForm.pipelines).toHaveLength(1)
    expect(store.deployForm.pipelines[0].name).toBe('only-one')
  })

  // --- Action: addRoute ---

  it('addRoute appends a new default route entry', () => {
    const store = usePipelinesStore()
    expect(store.deployForm.routes).toEqual([])
    store.addRoute()
    expect(store.deployForm.routes).toHaveLength(1)
    expect(store.deployForm.routes[0]).toEqual({
      source: '',
      target: '',
      eventTypes: '',
      filter: '',
    })
  })

  // --- Action: removeRoute ---

  it('removeRoute removes the route at the given index', () => {
    const store = usePipelinesStore()
    store.addRoute()
    store.addRoute()
    store.deployForm.routes[0].source = 'pipe-a'
    store.deployForm.routes[1].source = 'pipe-b'
    store.removeRoute(0)
    expect(store.deployForm.routes).toHaveLength(1)
    expect(store.deployForm.routes[0].source).toBe('pipe-b')
  })

  it('removeRoute allows removing the last route (routes can be empty)', () => {
    const store = usePipelinesStore()
    store.addRoute()
    expect(store.deployForm.routes).toHaveLength(1)
    store.removeRoute(0)
    expect(store.deployForm.routes).toHaveLength(0)
  })

  // --- Action: openDeployDialog / closeDeployDialog ---

  it('openDeployDialog resets form and opens dialog', () => {
    const store = usePipelinesStore()
    store.deployForm.name = 'dirty'
    store.deployDialogOpen = false
    store.openDeployDialog()
    expect(store.deployDialogOpen).toBe(true)
    expect(store.deployForm.name).toBe('')
  })

  it('closeDeployDialog closes the dialog', () => {
    const store = usePipelinesStore()
    store.deployDialogOpen = true
    store.closeDeployDialog()
    expect(store.deployDialogOpen).toBe(false)
  })

  // --- Action: selectGroup ---

  it('selectGroup sets the selected group', () => {
    const store = usePipelinesStore()
    const group = makeGroup('g1', 'running')
    store.selectGroup(group)
    expect(store.selectedGroup).toEqual(group)
  })

  it('selectGroup can clear the selection with null', () => {
    const store = usePipelinesStore()
    store.selectGroup(makeGroup('g1', 'running'))
    store.selectGroup(null)
    expect(store.selectedGroup).toBeNull()
  })

  // --- Action: clearError ---

  it('clearError sets error to null', () => {
    const store = usePipelinesStore()
    store.error = 'deploy failed'
    store.clearError()
    expect(store.error).toBeNull()
  })
})

// --- Helpers ---

function makeGroup(id: string, status: string): PipelineGroup {
  return {
    id,
    name: `group-${id}`,
    status: status as PipelineGroup['status'],
    pipeline_count: 1,
    placements: [],
  }
}
