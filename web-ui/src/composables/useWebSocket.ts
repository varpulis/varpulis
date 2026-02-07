import { ref, onMounted, onUnmounted, computed } from 'vue'
import { getWebSocketClient } from '@/api/websocket'
import type { ServerMessage, ConnectionState } from '@/types/websocket'

export interface UseWebSocketOptions {
  autoConnect?: boolean
  topics?: string[]
}

export function useWebSocket(options: UseWebSocketOptions = {}) {
  const { autoConnect = true, topics = [] } = options

  const client = getWebSocketClient()
  const connectionState = ref<ConnectionState>('disconnected')
  const lastMessage = ref<ServerMessage | null>(null)
  const error = ref<string | null>(null)

  const connected = computed(() => connectionState.value === 'connected')
  const connecting = computed(() => connectionState.value === 'connecting')

  // Cleanup functions
  const cleanupFns: Array<() => void> = []

  function connect(): void {
    client.connect()
  }

  function disconnect(): void {
    client.disconnect()
  }

  function send(eventType: string, data: Record<string, unknown>): void {
    client.injectEvent(eventType, data)
  }

  function subscribe(newTopics: string[]): void {
    client.subscribe(newTopics)
  }

  function unsubscribe(oldTopics: string[]): void {
    client.unsubscribe(oldTopics)
  }

  function requestMetrics(): void {
    client.requestMetrics()
  }

  function requestStreams(): void {
    client.requestStreams()
  }

  // Register message handler
  function onMessage(handler: (message: ServerMessage) => void): () => void {
    return client.onMessage(handler)
  }

  // Lifecycle
  onMounted(() => {
    // Register connection state handler
    const unsubscribeConnection = client.onConnectionChange((state) => {
      connectionState.value = state
      if (state === 'error') {
        error.value = 'WebSocket connection error'
      } else {
        error.value = null
      }
    })
    cleanupFns.push(unsubscribeConnection)

    // Register message handler to update lastMessage
    const unsubscribeMessage = client.onMessage((message) => {
      lastMessage.value = message
    })
    cleanupFns.push(unsubscribeMessage)

    // Auto connect if enabled
    if (autoConnect) {
      connect()
    }

    // Auto subscribe to topics
    if (topics.length > 0 && connected.value) {
      subscribe(topics)
    }
  })

  onUnmounted(() => {
    // Cleanup all handlers
    cleanupFns.forEach((fn) => fn())

    // Unsubscribe from topics
    if (topics.length > 0) {
      unsubscribe(topics)
    }
  })

  return {
    // State
    connectionState,
    connected,
    connecting,
    lastMessage,
    error,

    // Methods
    connect,
    disconnect,
    send,
    subscribe,
    unsubscribe,
    onMessage,
    requestMetrics,
    requestStreams,
  }
}
