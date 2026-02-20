import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useClusterStore } from '@/stores/cluster'
import type { Alert } from '@/types/cluster'
import type { Worker } from '@/types/cluster'
import type { PipelineGroup } from '@/types/pipeline'
import type { Migration } from '@/types/cluster'

describe('useClusterStore', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  // --- Initial State ---

  it('has empty initial state', () => {
    const store = useClusterStore()
    expect(store.workers).toEqual([])
    expect(store.selectedWorker).toBeNull()
    expect(store.pipelineGroups).toEqual([])
    expect(store.selectedGroup).toBeNull()
    expect(store.topology).toBeNull()
    expect(store.summary).toBeNull()
    expect(store.clusterHealth).toBeNull()
    expect(store.raftStatus).toBeNull()
    expect(store.migrations).toEqual([])
    expect(store.alerts).toEqual([])
    expect(store.loading).toBe(false)
    expect(store.error).toBeNull()
  })

  // --- Computed: healthyWorkers ---

  it('healthyWorkers filters workers with ready status', () => {
    const store = useClusterStore()
    store.workers = [
      makeWorker('w1', 'ready'),
      makeWorker('w2', 'unhealthy'),
      makeWorker('w3', 'ready'),
      makeWorker('w4', 'draining'),
    ]
    expect(store.healthyWorkers).toHaveLength(2)
    expect(store.healthyWorkers.map((w) => w.id)).toEqual(['w1', 'w3'])
  })

  it('healthyWorkers returns empty array when no workers', () => {
    const store = useClusterStore()
    expect(store.healthyWorkers).toEqual([])
  })

  // --- Computed: unhealthyWorkers ---

  it('unhealthyWorkers filters workers with unhealthy status', () => {
    const store = useClusterStore()
    store.workers = [
      makeWorker('w1', 'ready'),
      makeWorker('w2', 'unhealthy'),
      makeWorker('w3', 'unhealthy'),
    ]
    expect(store.unhealthyWorkers).toHaveLength(2)
    expect(store.unhealthyWorkers.map((w) => w.id)).toEqual(['w2', 'w3'])
  })

  // --- Computed: drainingWorkers ---

  it('drainingWorkers filters workers with draining status', () => {
    const store = useClusterStore()
    store.workers = [
      makeWorker('w1', 'ready'),
      makeWorker('w2', 'draining'),
    ]
    expect(store.drainingWorkers).toHaveLength(1)
    expect(store.drainingWorkers[0].id).toBe('w2')
  })

  // --- Computed: activeGroups ---

  it('activeGroups filters pipeline groups with running status', () => {
    const store = useClusterStore()
    store.pipelineGroups = [
      makeGroup('g1', 'running'),
      makeGroup('g2', 'failed'),
      makeGroup('g3', 'running'),
      makeGroup('g4', 'deploying'),
    ]
    expect(store.activeGroups).toHaveLength(2)
    expect(store.activeGroups.map((g) => g.id)).toEqual(['g1', 'g3'])
  })

  // --- Computed: activeMigrations ---

  it('activeMigrations excludes completed and failed migrations', () => {
    const store = useClusterStore()
    store.migrations = [
      makeMigration('m1', 'checkpointing'),
      makeMigration('m2', 'completed'),
      makeMigration('m3', 'deploying'),
      makeMigration('m4', 'failed'),
      makeMigration('m5', 'restoring'),
    ]
    expect(store.activeMigrations).toHaveLength(3)
    expect(store.activeMigrations.map((m) => m.id)).toEqual(['m1', 'm3', 'm5'])
  })

  // --- Computed: recentAlerts ---

  it('recentAlerts returns up to 10 alerts sorted newest first', () => {
    const store = useClusterStore()
    // Add 12 alerts with ascending timestamps
    for (let i = 0; i < 12; i++) {
      store.addAlert(makeAlert(`a${i}`, new Date(2026, 0, 1, 0, 0, i).toISOString()))
    }
    expect(store.recentAlerts).toHaveLength(10)
    // First alert should be the newest (a11)
    expect(store.recentAlerts[0].id).toBe('a11')
    // Last alert should be a2 (indices 0 and 1 are dropped because only 10 fit)
    expect(store.recentAlerts[9].id).toBe('a2')
  })

  it('recentAlerts returns all alerts when fewer than 10', () => {
    const store = useClusterStore()
    store.addAlert(makeAlert('a1', '2026-01-01T00:00:00Z'))
    store.addAlert(makeAlert('a2', '2026-01-02T00:00:00Z'))
    expect(store.recentAlerts).toHaveLength(2)
    expect(store.recentAlerts[0].id).toBe('a2')
    expect(store.recentAlerts[1].id).toBe('a1')
  })

  // --- Action: addAlert ---

  it('addAlert pushes a new alert to the alerts array', () => {
    const store = useClusterStore()
    const alert = makeAlert('test-alert', '2026-01-01T00:00:00Z')
    store.addAlert(alert)
    expect(store.alerts).toHaveLength(1)
    expect(store.alerts[0].id).toBe('test-alert')
  })

  // --- Action: acknowledgeAlert ---

  it('acknowledgeAlert marks the alert as acknowledged', () => {
    const store = useClusterStore()
    store.addAlert(makeAlert('a1', '2026-01-01T00:00:00Z'))
    expect(store.alerts[0].acknowledged).toBe(false)
    store.acknowledgeAlert('a1')
    expect(store.alerts[0].acknowledged).toBe(true)
  })

  it('acknowledgeAlert does nothing for nonexistent alert id', () => {
    const store = useClusterStore()
    store.addAlert(makeAlert('a1', '2026-01-01T00:00:00Z'))
    store.acknowledgeAlert('nonexistent')
    expect(store.alerts[0].acknowledged).toBe(false)
  })

  // --- Action: clearAlerts ---

  it('clearAlerts empties the alerts array', () => {
    const store = useClusterStore()
    store.addAlert(makeAlert('a1', '2026-01-01T00:00:00Z'))
    store.addAlert(makeAlert('a2', '2026-01-02T00:00:00Z'))
    expect(store.alerts).toHaveLength(2)
    store.clearAlerts()
    expect(store.alerts).toEqual([])
  })

  // --- Action: clearError ---

  it('clearError sets error to null', () => {
    const store = useClusterStore()
    store.error = 'something went wrong'
    store.clearError()
    expect(store.error).toBeNull()
  })
})

// --- Helpers ---

function makeWorker(id: string, status: 'ready' | 'unhealthy' | 'draining'): Worker {
  return {
    id,
    address: `http://worker-${id}:9000`,
    status,
    pipelines_running: 0,
    max_pipelines: 10,
    assigned_pipelines: [],
  }
}

function makeGroup(id: string, status: string): PipelineGroup {
  return {
    id,
    name: `group-${id}`,
    status: status as PipelineGroup['status'],
    pipeline_count: 1,
    placements: [],
  }
}

function makeMigration(id: string, status: string): Migration {
  return {
    id,
    pipeline_name: `pipeline-${id}`,
    group_id: `group-${id}`,
    source_worker: 'w1',
    target_worker: 'w2',
    status,
    reason: 'rebalance',
    elapsed_ms: 100,
  }
}

function makeAlert(id: string, timestamp: string): Alert {
  return {
    id,
    severity: 'warning',
    title: `Alert ${id}`,
    message: `Message for ${id}`,
    timestamp,
    source: 'test',
    acknowledged: false,
  }
}
