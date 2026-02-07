import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { ConnectionState, ServerMessage, EventLogEntry } from '@/types/websocket'
import { getWebSocketClient } from '@/api/websocket'
import { useMetricsStore } from './metrics'
import { useClusterStore } from './cluster'
import api from '@/api'

export const useWebSocketStore = defineStore('websocket', () => {
  // State
  const connectionState = ref<ConnectionState>('disconnected')
  const apiConnected = ref(false) // REST API connection status
  const reconnectAttempts = ref(0)
  const lastMessageTime = ref<Date | null>(null)
  const subscriptions = ref<Set<string>>(new Set())
  const error = ref<string | null>(null)
  const eventLog = ref<EventLogEntry[]>([])
  const showConnectionSnackbar = ref(false)
  let healthCheckInterval: ReturnType<typeof setInterval> | null = null

  // Computed - consider API connected if either WebSocket or REST API works
  const connected = computed(() => connectionState.value === 'connected' || apiConnected.value)
  const connecting = computed(() => connectionState.value === 'connecting')

  // Get WebSocket client
  const client = getWebSocketClient()

  // Actions
  function connect(): void {
    client.onConnectionChange((state) => {
      const wasConnected = connected.value
      connectionState.value = state

      if (state === 'connected' && !wasConnected) {
        showConnectionSnackbar.value = true
        reconnectAttempts.value = 0
        // Auto-subscribe to metrics
        subscribe(['metrics', 'alerts'])
      } else if (state === 'disconnected' && wasConnected) {
        showConnectionSnackbar.value = true
      } else if (state === 'error') {
        reconnectAttempts.value++
      }
    })

    client.onMessage(handleMessage)
    client.connect()
  }

  function disconnect(): void {
    client.disconnect()
    subscriptions.value.clear()
  }

  function subscribe(topics: string[]): void {
    if (!connected.value) return
    client.subscribe(topics)
    topics.forEach((t) => subscriptions.value.add(t))
  }

  function unsubscribe(topics: string[]): void {
    if (!connected.value) return
    client.unsubscribe(topics)
    topics.forEach((t) => subscriptions.value.delete(t))
  }

  function requestMetrics(): void {
    if (!connected.value) return
    client.requestMetrics()
  }

  function requestStreams(): void {
    if (!connected.value) return
    client.requestStreams()
  }

  function injectEvent(eventType: string, data: Record<string, unknown>): void {
    if (!connected.value) return
    client.injectEvent(eventType, data)
  }

  function loadFile(path: string): void {
    if (!connected.value) return
    client.loadFile(path)
  }

  function handleMessage(message: ServerMessage): void {
    lastMessageTime.value = new Date()

    switch (message.type) {
      case 'metrics':
        handleMetricsMessage(message)
        break
      case 'output_event':
        handleOutputEvent(message)
        break
      case 'error':
        handleError(message)
        break
      case 'worker_status':
        handleWorkerStatus(message)
        break
      case 'alert':
        handleAlert(message)
        break
      case 'event_injected':
        handleEventInjected(message)
        break
      case 'streams':
        // Handle streams message
        break
      case 'load_result':
        // Handle load result
        break
    }
  }

  function handleMetricsMessage(message: Extract<ServerMessage, { type: 'metrics' }>): void {
    const metricsStore = useMetricsStore()
    metricsStore.updateMetrics(message.data)
  }

  function handleOutputEvent(message: Extract<ServerMessage, { type: 'output_event' }>): void {
    eventLog.value.push({
      id: `evt-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      direction: 'out',
      eventType: message.event_type,
      data: message.data,
      timestamp: new Date(message.timestamp),
      pipelineId: message.pipeline_id,
    })

    // Limit event log size
    if (eventLog.value.length > 1000) {
      eventLog.value = eventLog.value.slice(-1000)
    }
  }

  function handleError(message: Extract<ServerMessage, { type: 'error' }>): void {
    error.value = `${message.code}: ${message.message}`
    console.error('WebSocket error:', message)
  }

  function handleWorkerStatus(message: Extract<ServerMessage, { type: 'worker_status' }>): void {
    const clusterStore = useClusterStore()
    const worker = clusterStore.workers.find((w) => w.id === message.worker_id)
    if (worker) {
      worker.status = message.status
    }
  }

  function handleAlert(message: Extract<ServerMessage, { type: 'alert' }>): void {
    const clusterStore = useClusterStore()
    clusterStore.addAlert({
      id: message.id,
      severity: message.severity,
      title: message.title,
      message: message.message,
      timestamp: new Date().toISOString(),
      source: message.source,
      acknowledged: false,
    })
  }

  function handleEventInjected(message: Extract<ServerMessage, { type: 'event_injected' }>): void {
    // Could emit an event or update state
    if (!message.success) {
      error.value = message.error || 'Event injection failed'
    }
  }

  function clearEventLog(): void {
    eventLog.value = []
  }

  function clearError(): void {
    error.value = null
  }

  // REST API health check
  async function checkApiHealth(): Promise<boolean> {
    try {
      await api.get('/cluster/workers')
      const wasConnected = apiConnected.value
      apiConnected.value = true

      if (!wasConnected) {
        showConnectionSnackbar.value = true
      }
      return true
    } catch {
      const wasConnected = apiConnected.value
      apiConnected.value = false

      if (wasConnected) {
        showConnectionSnackbar.value = true
      }
      return false
    }
  }

  // Start periodic health check
  function startHealthCheck(intervalMs = 10000): void {
    if (healthCheckInterval) {
      clearInterval(healthCheckInterval)
    }
    checkApiHealth() // Check immediately
    healthCheckInterval = setInterval(checkApiHealth, intervalMs)
  }

  // Stop health check
  function stopHealthCheck(): void {
    if (healthCheckInterval) {
      clearInterval(healthCheckInterval)
      healthCheckInterval = null
    }
  }

  return {
    // State
    connectionState,
    apiConnected,
    reconnectAttempts,
    lastMessageTime,
    subscriptions,
    error,
    eventLog,
    showConnectionSnackbar,

    // Computed
    connected,
    connecting,

    // Actions
    connect,
    disconnect,
    subscribe,
    unsubscribe,
    requestMetrics,
    requestStreams,
    injectEvent,
    loadFile,
    clearEventLog,
    clearError,
    checkApiHealth,
    startHealthCheck,
    stopHealthCheck,
  }
})
