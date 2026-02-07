// WebSocket message types (matching Rust server)

// === Client to Server Messages ===

export interface LoadFileMessage {
  type: 'load_file'
  path: string
}

export interface InjectEventMessage {
  type: 'inject_event'
  event_type: string
  data: Record<string, unknown>
}

export interface GetStreamsMessage {
  type: 'get_streams'
}

export interface GetMetricsMessage {
  type: 'get_metrics'
}

export interface SubscribeMessage {
  type: 'subscribe'
  topics: string[]
}

export interface UnsubscribeMessage {
  type: 'unsubscribe'
  topics: string[]
}

export type ClientMessage =
  | LoadFileMessage
  | InjectEventMessage
  | GetStreamsMessage
  | GetMetricsMessage
  | SubscribeMessage
  | UnsubscribeMessage

// === Server to Client Messages ===

export interface LoadResultMessage {
  type: 'load_result'
  success: boolean
  pipeline_id?: string
  error?: string
}

export interface StreamsMessage {
  type: 'streams'
  streams: StreamInfo[]
}

export interface StreamInfo {
  id: string
  name: string
  event_count: number
  last_event_time?: string
}

export interface MetricsMessage {
  type: 'metrics'
  data: {
    events_processed: number
    events_emitted: number
    errors: number
    active_streams: number
    uptime_secs: number
    throughput_eps: number
    avg_latency_ms: number
  }
}

export interface OutputEventMessage {
  type: 'output_event'
  event_type: string
  data: Record<string, unknown>
  timestamp: string
  pipeline_id?: string
}

export interface EventInjectedMessage {
  type: 'event_injected'
  success: boolean
  routed_to?: string[]
  error?: string
}

export interface ErrorMessage {
  type: 'error'
  code: string
  message: string
}

export interface WorkerStatusMessage {
  type: 'worker_status'
  worker_id: string
  status: 'ready' | 'unhealthy' | 'draining'
  timestamp: string
}

export interface AlertMessage {
  type: 'alert'
  id: string
  severity: 'info' | 'warning' | 'error' | 'critical'
  title: string
  message: string
  source: string
}

export type ServerMessage =
  | LoadResultMessage
  | StreamsMessage
  | MetricsMessage
  | OutputEventMessage
  | EventInjectedMessage
  | ErrorMessage
  | WorkerStatusMessage
  | AlertMessage

// === Connection State ===

export type ConnectionState = 'connecting' | 'connected' | 'disconnected' | 'error'

export interface WebSocketState {
  connectionState: ConnectionState
  connected: boolean
  reconnectAttempts: number
  lastMessageTime: Date | null
  subscriptions: Set<string>
  error: string | null
}

// === Event Log Entry ===

export interface EventLogEntry {
  id: string
  direction: 'in' | 'out'
  eventType: string
  data: Record<string, unknown>
  timestamp: Date
  pipelineId?: string
}
