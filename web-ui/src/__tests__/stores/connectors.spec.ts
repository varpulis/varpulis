import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useConnectorsStore } from '@/stores/connectors'

describe('useConnectorsStore', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  // --- Initial State ---

  it('has empty initial state', () => {
    const store = useConnectorsStore()
    expect(store.connectors).toEqual([])
    expect(store.loading).toBe(false)
    expect(store.error).toBeNull()
  })

  // --- Computed: connectorNames ---

  it('connectorNames returns names of all connectors', () => {
    const store = useConnectorsStore()
    store.connectors = [
      { name: 'mqtt-source', connector_type: 'mqtt', params: { url: 'tcp://localhost:1883' } },
      { name: 'kafka-sink', connector_type: 'kafka', params: { url: 'localhost:9092' } },
    ]
    expect(store.connectorNames).toEqual(['mqtt-source', 'kafka-sink'])
  })

  it('connectorNames returns empty array when no connectors', () => {
    const store = useConnectorsStore()
    expect(store.connectorNames).toEqual([])
  })

  // --- Action: clearError ---

  it('clearError sets error to null', () => {
    const store = useConnectorsStore()
    store.error = 'failed to create connector'
    store.clearError()
    expect(store.error).toBeNull()
  })

  // --- State mutations ---

  it('connectors can be set directly', () => {
    const store = useConnectorsStore()
    store.connectors = [
      { name: 'test-conn', connector_type: 'mqtt', params: { url: 'tcp://localhost:1883' } },
    ]
    expect(store.connectors).toHaveLength(1)
    expect(store.connectors[0].name).toBe('test-conn')
  })

  it('connectors can be filtered to remove by name', () => {
    const store = useConnectorsStore()
    store.connectors = [
      { name: 'conn-a', connector_type: 'mqtt', params: {} },
      { name: 'conn-b', connector_type: 'kafka', params: {} },
      { name: 'conn-c', connector_type: 'nats', params: {} },
    ]
    store.connectors = store.connectors.filter((c) => c.name !== 'conn-b')
    expect(store.connectors).toHaveLength(2)
    expect(store.connectors.map((c) => c.name)).toEqual(['conn-a', 'conn-c'])
  })

  it('connectors can be updated in place by name', () => {
    const store = useConnectorsStore()
    store.connectors = [
      { name: 'conn-a', connector_type: 'mqtt', params: { url: 'old-url' } },
    ]
    const index = store.connectors.findIndex((c) => c.name === 'conn-a')
    if (index !== -1) {
      store.connectors[index] = { name: 'conn-a', connector_type: 'mqtt', params: { url: 'new-url' } }
    }
    expect(store.connectors[0].params.url).toBe('new-url')
  })

  it('loading state can be toggled', () => {
    const store = useConnectorsStore()
    store.loading = true
    expect(store.loading).toBe(true)
    store.loading = false
    expect(store.loading).toBe(false)
  })
})
