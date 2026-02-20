import { describe, it, expect, beforeEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// Mock the WebSocket client module before importing useWebSocket
const mockClient = {
  connect: vi.fn(),
  disconnect: vi.fn(),
  injectEvent: vi.fn(),
  subscribe: vi.fn(),
  unsubscribe: vi.fn(),
  requestMetrics: vi.fn(),
  requestStreams: vi.fn(),
  onMessage: vi.fn().mockReturnValue(vi.fn()),
  onConnectionChange: vi.fn().mockReturnValue(vi.fn()),
  getConnectionState: vi.fn().mockReturnValue('disconnected'),
}

vi.mock('@/api/websocket', () => ({
  getWebSocketClient: () => mockClient,
}))

/**
 * The useWebSocket composable uses onMounted/onUnmounted lifecycle hooks.
 * Outside a component context (in unit tests), these hooks emit Vue warnings
 * but the returned functions and refs still work. We test the pure logic:
 * connect/disconnect delegation, send, subscribe/unsubscribe, and
 * the request helpers.
 */
import { useWebSocket } from '@/composables/useWebSocket'

describe('useWebSocket', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.clearAllMocks()
    // Reset default mock return values
    mockClient.onMessage.mockReturnValue(vi.fn())
    mockClient.onConnectionChange.mockReturnValue(vi.fn())
    mockClient.getConnectionState.mockReturnValue('disconnected')
  })

  // --- Returned state ---

  it('returns initial disconnected state', () => {
    const { connectionState, connected, connecting, lastMessage, error } = useWebSocket({
      autoConnect: false,
    })

    expect(connectionState.value).toBe('disconnected')
    expect(connected.value).toBe(false)
    expect(connecting.value).toBe(false)
    expect(lastMessage.value).toBeNull()
    expect(error.value).toBeNull()
  })

  it('connected is true when connectionState is connected', () => {
    const { connectionState, connected } = useWebSocket({ autoConnect: false })
    connectionState.value = 'connected'
    expect(connected.value).toBe(true)
  })

  it('connecting is true when connectionState is connecting', () => {
    const { connectionState, connecting } = useWebSocket({ autoConnect: false })
    connectionState.value = 'connecting'
    expect(connecting.value).toBe(true)
  })

  // --- connect / disconnect ---

  it('connect delegates to client.connect()', () => {
    const { connect } = useWebSocket({ autoConnect: false })
    connect()
    expect(mockClient.connect).toHaveBeenCalledOnce()
  })

  it('disconnect delegates to client.disconnect()', () => {
    const { disconnect } = useWebSocket({ autoConnect: false })
    disconnect()
    expect(mockClient.disconnect).toHaveBeenCalledOnce()
  })

  // --- send ---

  it('send delegates to client.injectEvent()', () => {
    const { send } = useWebSocket({ autoConnect: false })
    send('TradeEvent', { symbol: 'MSFT', price: 400 })
    expect(mockClient.injectEvent).toHaveBeenCalledWith('TradeEvent', {
      symbol: 'MSFT',
      price: 400,
    })
  })

  // --- subscribe / unsubscribe ---

  it('subscribe delegates to client.subscribe()', () => {
    const { subscribe } = useWebSocket({ autoConnect: false })
    subscribe(['metrics', 'alerts'])
    expect(mockClient.subscribe).toHaveBeenCalledWith(['metrics', 'alerts'])
  })

  it('unsubscribe delegates to client.unsubscribe()', () => {
    const { unsubscribe } = useWebSocket({ autoConnect: false })
    unsubscribe(['metrics'])
    expect(mockClient.unsubscribe).toHaveBeenCalledWith(['metrics'])
  })

  // --- requestMetrics / requestStreams ---

  it('requestMetrics delegates to client.requestMetrics()', () => {
    const { requestMetrics } = useWebSocket({ autoConnect: false })
    requestMetrics()
    expect(mockClient.requestMetrics).toHaveBeenCalledOnce()
  })

  it('requestStreams delegates to client.requestStreams()', () => {
    const { requestStreams } = useWebSocket({ autoConnect: false })
    requestStreams()
    expect(mockClient.requestStreams).toHaveBeenCalledOnce()
  })

  // --- onMessage ---

  it('onMessage delegates to client.onMessage()', () => {
    const unsub = vi.fn()
    mockClient.onMessage.mockReturnValue(unsub)

    const { onMessage } = useWebSocket({ autoConnect: false })
    const handler = vi.fn()
    const result = onMessage(handler)

    expect(mockClient.onMessage).toHaveBeenCalledWith(handler)
    expect(result).toBe(unsub)
  })

  // --- default options ---

  it('defaults autoConnect to true', () => {
    // useWebSocket() with no args — autoConnect defaults to true
    // onMounted won't fire outside a component, so we just verify no error
    expect(() => useWebSocket()).not.toThrow()
  })

  it('accepts topics option', () => {
    // topics are used in onMounted, but we can verify the composable accepts them
    expect(() => useWebSocket({ topics: ['events', 'alerts'] })).not.toThrow()
  })
})
