import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type {
  ActorHealth,
  ConnectorHealth,
  SystemHealth,
} from '@/types/health'
import api from '@/api/index'

export const useHealthStore = defineStore('health', () => {
  // State
  const actors = ref<ActorHealth[]>([])
  const connectors = ref<ConnectorHealth[]>([])
  const systemStatus = ref<'healthy' | 'degraded' | 'unhealthy'>('healthy')
  const uptimeSecs = ref(0)
  const lastUpdated = ref<Date | null>(null)
  const loading = ref(false)
  const error = ref<string | null>(null)
  const pollingInterval = ref<ReturnType<typeof setInterval> | null>(null)

  // Computed
  const healthyActors = computed(() =>
    actors.value.filter((a) => a.status === 'healthy')
  )

  const degradedActors = computed(() =>
    actors.value.filter((a) => a.status === 'degraded')
  )

  const downActors = computed(() =>
    actors.value.filter((a) => a.status === 'down')
  )

  const connectedConnectors = computed(() =>
    connectors.value.filter((c) => c.connected)
  )

  const disconnectedConnectors = computed(() =>
    connectors.value.filter((c) => !c.connected)
  )

  const totalRestarts = computed(() =>
    actors.value.reduce((sum, a) => sum + a.restarts, 0)
  )

  // Actions

  /** Fetch health from the REST endpoint */
  async function fetchHealth(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      const response = await api.get<SystemHealth>('/health')
      applyHealthUpdate(response.data)
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch health'
      console.error('Failed to fetch health:', e)
    } finally {
      loading.value = false
    }
  }

  /** Apply a health update (from REST or WebSocket) */
  function applyHealthUpdate(health: Partial<SystemHealth>): void {
    if (health.actors) {
      actors.value = health.actors
    }
    if (health.connectors) {
      connectors.value = health.connectors
    }
    if (health.status) {
      systemStatus.value = health.status
    }
    if (health.uptime_secs !== undefined) {
      uptimeSecs.value = health.uptime_secs
    }
    lastUpdated.value = new Date()
  }

  /** Handle WebSocket actor_health message */
  function handleActorHealthMessage(data: {
    actors: ActorHealth[]
    connectors: ConnectorHealth[]
    timestamp: string
  }): void {
    actors.value = data.actors
    connectors.value = data.connectors
    lastUpdated.value = new Date(data.timestamp)
  }

  /** Start periodic polling (fallback when WebSocket is unavailable) */
  function startPolling(intervalMs: number = 10000): void {
    stopPolling()
    fetchHealth()
    pollingInterval.value = setInterval(fetchHealth, intervalMs)
  }

  /** Stop periodic polling */
  function stopPolling(): void {
    if (pollingInterval.value) {
      clearInterval(pollingInterval.value)
      pollingInterval.value = null
    }
  }

  return {
    // State
    actors,
    connectors,
    systemStatus,
    uptimeSecs,
    lastUpdated,
    loading,
    error,

    // Computed
    healthyActors,
    degradedActors,
    downActors,
    connectedConnectors,
    disconnectedConnectors,
    totalRestarts,

    // Actions
    fetchHealth,
    applyHealthUpdate,
    handleActorHealthMessage,
    startPolling,
    stopPolling,
  }
})
