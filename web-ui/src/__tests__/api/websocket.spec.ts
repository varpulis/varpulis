import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { WebSocketClient, getWebSocketClient, resetWebSocketClient } from '@/api/websocket'

// Mock ReconnectingWebSocket
let mockWsInstance: {
  close: ReturnType<typeof vi.fn>
  send: ReturnType<typeof vi.fn>
  onopen: ((event?: unknown) => void) | null
  onclose: ((event?: unknown) => void) | null
  onerror: ((event?: unknown) => void) | null
  onmessage: ((event: { data: string }) => void) | null
}

vi.mock('reconnecting-websocket', () => {
  return {
    default: class MockReconnectingWebSocket {
      close = vi.fn()
      send = vi.fn()
      onopen: ((event?: unknown) => void) | null = null
      onclose: ((event?: unknown) => void) | null = null
      onerror: ((event?: unknown) => void) | null = null
      onmessage: ((event: { data: string }) => void) | null = null

      constructor() {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        mockWsInstance = this as typeof mockWsInstance
      }
    },
  }
})

describe('WebSocketClient', () => {
  let client: WebSocketClient

  beforeEach(() => {
    client = new WebSocketClient('ws://localhost:9100')
  })

  afterEach(() => {
    client.disconnect()
  })

  // --- Connection State ---

  it('initial state is disconnected', () => {
    expect(client.getConnectionState()).toBe('disconnected')
  })

  it('connect sets state to connecting then connected on open', () => {
    client.connect()
    expect(client.getConnectionState()).toBe('connecting')

    mockWsInstance.onopen!()
    expect(client.getConnectionState()).toBe('connected')
  })

  it('connect is idempotent — second call is a no-op', () => {
    client.connect()
    const firstInstance = mockWsInstance

    client.connect()
    // mockWsInstance would be overwritten if a new RWS was created
    expect(mockWsInstance).toBe(firstInstance)
  })

  it('disconnect sets state to disconnected and nullifies ws', () => {
    client.connect()
    mockWsInstance.onopen!()
    expect(client.getConnectionState()).toBe('connected')

    client.disconnect()
    expect(client.getConnectionState()).toBe('disconnected')
    expect(mockWsInstance.close).toHaveBeenCalled()
  })

  it('disconnect is safe to call when not connected', () => {
    expect(() => client.disconnect()).not.toThrow()
  })

  it('onclose sets state to disconnected', () => {
    client.connect()
    mockWsInstance.onopen!()
    mockWsInstance.onclose!()
    expect(client.getConnectionState()).toBe('disconnected')
  })

  it('onerror sets state to error', () => {
    client.connect()
    mockWsInstance.onerror!({ message: 'fail' })
    expect(client.getConnectionState()).toBe('error')
  })

  // --- Connection State Handlers ---

  it('onConnectionChange fires immediately with current state', () => {
    const handler = vi.fn()
    client.onConnectionChange(handler)
    expect(handler).toHaveBeenCalledWith('disconnected')
  })

  it('onConnectionChange fires on state transitions', () => {
    const states: string[] = []
    client.onConnectionChange((s) => states.push(s))

    // Initial call
    expect(states).toEqual(['disconnected'])

    client.connect()
    expect(states).toEqual(['disconnected', 'connecting'])

    mockWsInstance.onopen!()
    expect(states).toEqual(['disconnected', 'connecting', 'connected'])
  })

  it('onConnectionChange returns unsubscribe function', () => {
    const handler = vi.fn()
    const unsub = client.onConnectionChange(handler)

    handler.mockClear()
    unsub()

    client.connect()
    // Handler should NOT be called after unsubscribe
    expect(handler).not.toHaveBeenCalled()
  })

  // --- Sending Messages ---

  it('send does nothing when not connected', () => {
    // Not connected — no ws instance
    client.send({ type: 'get_metrics' })
    // No error thrown, nothing sent
  })

  it('send serializes and sends message when connected', () => {
    client.connect()
    mockWsInstance.onopen!()

    client.send({ type: 'get_metrics' })
    expect(mockWsInstance.send).toHaveBeenCalledWith('{"type":"get_metrics"}')
  })

  it('requestMetrics sends get_metrics message', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.requestMetrics()
    expect(mockWsInstance.send).toHaveBeenCalledWith('{"type":"get_metrics"}')
  })

  it('requestStreams sends get_streams message', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.requestStreams()
    expect(mockWsInstance.send).toHaveBeenCalledWith('{"type":"get_streams"}')
  })

  it('loadFile sends load_file message with path', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.loadFile('/tmp/test.vpl')
    expect(mockWsInstance.send).toHaveBeenCalledWith(
      JSON.stringify({ type: 'load_file', path: '/tmp/test.vpl' })
    )
  })

  it('subscribe sends subscribe message with topics', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.subscribe(['metrics', 'alerts'])
    expect(mockWsInstance.send).toHaveBeenCalledWith(
      JSON.stringify({ type: 'subscribe', topics: ['metrics', 'alerts'] })
    )
  })

  it('unsubscribe sends unsubscribe message with topics', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.unsubscribe(['metrics'])
    expect(mockWsInstance.send).toHaveBeenCalledWith(
      JSON.stringify({ type: 'unsubscribe', topics: ['metrics'] })
    )
  })

  // --- Message Handling ---

  it('onMessage handler receives parsed server messages', () => {
    const handler = vi.fn()
    client.onMessage(handler)

    client.connect()

    const msg = { type: 'metrics', data: { events_processed: 100 } }
    mockWsInstance.onmessage!({ data: JSON.stringify(msg) })

    expect(handler).toHaveBeenCalledWith(msg)
  })

  it('onMessage returns unsubscribe function', () => {
    const handler = vi.fn()
    const unsub = client.onMessage(handler)

    client.connect()

    unsub()

    mockWsInstance.onmessage!({ data: '{"type":"metrics","data":{}}' })
    expect(handler).not.toHaveBeenCalled()
  })

  it('multiple message handlers all receive messages', () => {
    const h1 = vi.fn()
    const h2 = vi.fn()
    client.onMessage(h1)
    client.onMessage(h2)

    client.connect()

    const msg = { type: 'metrics', data: {} }
    mockWsInstance.onmessage!({ data: JSON.stringify(msg) })

    expect(h1).toHaveBeenCalledOnce()
    expect(h2).toHaveBeenCalledOnce()
  })

  it('handler errors do not prevent other handlers from running', () => {
    const errorHandler = vi.fn().mockImplementation(() => {
      throw new Error('handler crash')
    })
    const goodHandler = vi.fn()

    client.onMessage(errorHandler)
    client.onMessage(goodHandler)

    client.connect()
    mockWsInstance.onmessage!({ data: '{"type":"metrics","data":{}}' })

    expect(errorHandler).toHaveBeenCalledOnce()
    expect(goodHandler).toHaveBeenCalledOnce()
  })

  it('invalid JSON messages are ignored', () => {
    const handler = vi.fn()
    client.onMessage(handler)

    client.connect()
    mockWsInstance.onmessage!({ data: 'not-json{{' })

    expect(handler).not.toHaveBeenCalled()
  })

  // --- Event Log ---

  it('injectEvent logs an inbound event', () => {
    client.connect()
    mockWsInstance.onopen!()

    client.injectEvent('TradeEvent', { symbol: 'AAPL', price: 150 })

    const log = client.getEventLog()
    expect(log).toHaveLength(1)
    expect(log[0].direction).toBe('in')
    expect(log[0].eventType).toBe('TradeEvent')
    expect(log[0].data).toEqual({ symbol: 'AAPL', price: 150 })
  })

  it('output_event messages are logged as outbound events', () => {
    client.connect()

    const msg = {
      type: 'output_event',
      event_type: 'Alert',
      data: { level: 'high' },
      timestamp: '2026-02-20T12:00:00Z',
      pipeline_id: 'p-1',
    }
    mockWsInstance.onmessage!({ data: JSON.stringify(msg) })

    const log = client.getEventLog()
    expect(log).toHaveLength(1)
    expect(log[0].direction).toBe('out')
    expect(log[0].eventType).toBe('Alert')
    expect(log[0].pipelineId).toBe('p-1')
  })

  it('getEventLog returns a copy', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.injectEvent('A', {})

    const log1 = client.getEventLog()
    const log2 = client.getEventLog()
    expect(log1).not.toBe(log2)
    expect(log1).toEqual(log2)
  })

  it('clearEventLog empties the log', () => {
    client.connect()
    mockWsInstance.onopen!()
    client.injectEvent('A', {})
    client.injectEvent('B', {})
    expect(client.getEventLog()).toHaveLength(2)

    client.clearEventLog()
    expect(client.getEventLog()).toHaveLength(0)
  })

  it('event log is trimmed at max size', () => {
    client.connect()
    mockWsInstance.onopen!()

    // maxEventLogSize is 1000 — inject 1002 events
    for (let i = 0; i < 1002; i++) {
      client.injectEvent('Evt', { i })
    }

    expect(client.getEventLog()).toHaveLength(1000)
    // Oldest two should have been dropped
    expect(client.getEventLog()[0].data).toEqual({ i: 2 })
  })
})

// --- Singleton ---

describe('WebSocketClient singleton', () => {
  afterEach(() => {
    resetWebSocketClient()
  })

  it('getWebSocketClient returns the same instance', () => {
    const a = getWebSocketClient()
    const b = getWebSocketClient()
    expect(a).toBe(b)
  })

  it('resetWebSocketClient creates a new instance on next call', () => {
    const a = getWebSocketClient()
    resetWebSocketClient()
    const b = getWebSocketClient()
    expect(a).not.toBe(b)
  })
})
