import ReconnectingWebSocket from 'reconnecting-websocket'
import type {
  ClientMessage,
  ServerMessage,
  ConnectionState,
  EventLogEntry,
} from '@/types/websocket'

type MessageHandler = (message: ServerMessage) => void
type ConnectionHandler = (state: ConnectionState) => void

export class WebSocketClient {
  private ws: ReconnectingWebSocket | null = null
  private messageHandlers: Set<MessageHandler> = new Set()
  private connectionHandlers: Set<ConnectionHandler> = new Set()
  private connectionState: ConnectionState = 'disconnected'
  private eventLog: EventLogEntry[] = []
  private maxEventLogSize = 1000
  private eventIdCounter = 0

  constructor(private baseUrl: string = '') {
    // Use current host if no base URL provided
    if (!baseUrl) {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      this.baseUrl = `${protocol}//${window.location.host}`
    }
  }

  /**
   * Connect to the WebSocket server
   */
  connect(): void {
    if (this.ws) {
      return
    }

    const wsUrl = `${this.baseUrl}/ws`
    this.setConnectionState('connecting')

    this.ws = new ReconnectingWebSocket(wsUrl, [], {
      maxRetries: 10,
      connectionTimeout: 5000,
      maxReconnectionDelay: 10000,
      minReconnectionDelay: 1000,
    })

    this.ws.onopen = () => {
      this.setConnectionState('connected')
      console.log('WebSocket connected')
    }

    this.ws.onclose = () => {
      this.setConnectionState('disconnected')
      console.log('WebSocket disconnected')
    }

    this.ws.onerror = (error) => {
      this.setConnectionState('error')
      console.error('WebSocket error:', error)
    }

    this.ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data) as ServerMessage
        this.handleMessage(message)
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e)
      }
    }
  }

  /**
   * Disconnect from the WebSocket server
   */
  disconnect(): void {
    if (this.ws) {
      this.ws.close()
      this.ws = null
      this.setConnectionState('disconnected')
    }
  }

  /**
   * Send a message to the server
   */
  send(message: ClientMessage): void {
    if (!this.ws || this.connectionState !== 'connected') {
      console.warn('WebSocket not connected, cannot send message')
      return
    }
    this.ws.send(JSON.stringify(message))
  }

  /**
   * Request metrics from server
   */
  requestMetrics(): void {
    this.send({ type: 'get_metrics' })
  }

  /**
   * Request streams from server
   */
  requestStreams(): void {
    this.send({ type: 'get_streams' })
  }

  /**
   * Load a VPL file
   */
  loadFile(path: string): void {
    this.send({ type: 'load_file', path })
  }

  /**
   * Inject an event
   */
  injectEvent(eventType: string, data: Record<string, unknown>): void {
    this.send({ type: 'inject_event', event_type: eventType, data })

    // Log injected event
    this.addEventLog({
      id: `evt-${++this.eventIdCounter}`,
      direction: 'in',
      eventType,
      data,
      timestamp: new Date(),
    })
  }

  /**
   * Subscribe to topics
   */
  subscribe(topics: string[]): void {
    this.send({ type: 'subscribe', topics })
  }

  /**
   * Unsubscribe from topics
   */
  unsubscribe(topics: string[]): void {
    this.send({ type: 'unsubscribe', topics })
  }

  /**
   * Register a message handler
   */
  onMessage(handler: MessageHandler): () => void {
    this.messageHandlers.add(handler)
    return () => this.messageHandlers.delete(handler)
  }

  /**
   * Register a connection state handler
   */
  onConnectionChange(handler: ConnectionHandler): () => void {
    this.connectionHandlers.add(handler)
    // Immediately call with current state
    handler(this.connectionState)
    return () => this.connectionHandlers.delete(handler)
  }

  /**
   * Get current connection state
   */
  getConnectionState(): ConnectionState {
    return this.connectionState
  }

  /**
   * Get event log
   */
  getEventLog(): EventLogEntry[] {
    return [...this.eventLog]
  }

  /**
   * Clear event log
   */
  clearEventLog(): void {
    this.eventLog = []
  }

  private setConnectionState(state: ConnectionState): void {
    this.connectionState = state
    this.connectionHandlers.forEach((handler) => handler(state))
  }

  private handleMessage(message: ServerMessage): void {
    // Log output events
    if (message.type === 'output_event') {
      this.addEventLog({
        id: `evt-${++this.eventIdCounter}`,
        direction: 'out',
        eventType: message.event_type,
        data: message.data,
        timestamp: new Date(message.timestamp),
        pipelineId: message.pipeline_id,
      })
    }

    // Notify all handlers
    this.messageHandlers.forEach((handler) => {
      try {
        handler(message)
      } catch (e) {
        console.error('Error in message handler:', e)
      }
    })
  }

  private addEventLog(entry: EventLogEntry): void {
    this.eventLog.push(entry)
    // Trim if exceeds max size
    if (this.eventLog.length > this.maxEventLogSize) {
      this.eventLog = this.eventLog.slice(-this.maxEventLogSize)
    }
  }
}

// Singleton instance
let wsClient: WebSocketClient | null = null

export function getWebSocketClient(): WebSocketClient {
  if (!wsClient) {
    wsClient = new WebSocketClient()
  }
  return wsClient
}

export function resetWebSocketClient(): void {
  if (wsClient) {
    wsClient.disconnect()
    wsClient = null
  }
}
