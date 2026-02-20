import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useWebSocketStore } from '@/stores/websocket'

describe('useWebSocketStore', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  // --- Initial State ---

  it('has correct initial state', () => {
    const store = useWebSocketStore()
    expect(store.connectionState).toBe('disconnected')
    expect(store.apiConnected).toBe(false)
    expect(store.reconnectAttempts).toBe(0)
    expect(store.lastMessageTime).toBeNull()
    expect(store.subscriptions.size).toBe(0)
    expect(store.error).toBeNull()
    expect(store.eventLog).toEqual([])
    expect(store.showConnectionSnackbar).toBe(false)
  })

  // --- Computed: connected ---

  it('connected is false when both ws and api are disconnected', () => {
    const store = useWebSocketStore()
    expect(store.connected).toBe(false)
  })

  it('connected is true when apiConnected is true', () => {
    const store = useWebSocketStore()
    store.apiConnected = true
    expect(store.connected).toBe(true)
  })

  it('connected is true when connectionState is connected', () => {
    const store = useWebSocketStore()
    store.connectionState = 'connected'
    expect(store.connected).toBe(true)
  })

  // --- Computed: connecting ---

  it('connecting is true only when connectionState is connecting', () => {
    const store = useWebSocketStore()
    expect(store.connecting).toBe(false)

    store.connectionState = 'connecting'
    expect(store.connecting).toBe(true)

    store.connectionState = 'connected'
    expect(store.connecting).toBe(false)
  })

  // --- State: event log ---

  it('clearEventLog empties the event log', () => {
    const store = useWebSocketStore()
    store.eventLog.push({
      id: 'evt-1',
      direction: 'out',
      eventType: 'TestEvent',
      data: { key: 'value' },
      timestamp: new Date(),
    })
    expect(store.eventLog).toHaveLength(1)
    store.clearEventLog()
    expect(store.eventLog).toEqual([])
  })

  // --- Action: clearError ---

  it('clearError sets error to null', () => {
    const store = useWebSocketStore()
    store.error = 'connection failed'
    store.clearError()
    expect(store.error).toBeNull()
  })

  // --- State mutations ---

  it('reconnectAttempts can be incremented', () => {
    const store = useWebSocketStore()
    store.reconnectAttempts = 3
    expect(store.reconnectAttempts).toBe(3)
  })

  it('subscriptions can be tracked manually', () => {
    const store = useWebSocketStore()
    store.subscriptions.add('metrics')
    store.subscriptions.add('alerts')
    expect(store.subscriptions.size).toBe(2)
    expect(store.subscriptions.has('metrics')).toBe(true)
    expect(store.subscriptions.has('alerts')).toBe(true)
    store.subscriptions.delete('metrics')
    expect(store.subscriptions.size).toBe(1)
  })

  it('showConnectionSnackbar can be toggled', () => {
    const store = useWebSocketStore()
    store.showConnectionSnackbar = true
    expect(store.showConnectionSnackbar).toBe(true)
    store.showConnectionSnackbar = false
    expect(store.showConnectionSnackbar).toBe(false)
  })
})
