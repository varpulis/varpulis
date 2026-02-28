// Actor health and observable state types
// Maps to the Rust ActorHealth/Health types from varpulis-actors

/** Health status of an individual actor */
export type ActorHealthStatus = 'healthy' | 'degraded' | 'down'

/** Health information for a single actor */
export interface ActorHealth {
  /** Actor instance name */
  name: string
  /** Current health status */
  status: ActorHealthStatus
  /** Number of times the supervisor has restarted this actor */
  restarts: number
  /** Seconds since the actor was last (re)started */
  uptime_secs: number
  /** Actor-specific observable state (varies by actor type) */
  observable_state: Record<string, unknown>
  /** Reason for degraded/down status, if applicable */
  reason?: string
}

/** Connector health report from the managed connector registry */
export interface ConnectorHealth {
  /** Connector instance name */
  name: string
  /** Connector type (mqtt, kafka, nats, etc.) */
  connector_type: string
  /** Whether the connector is currently connected */
  connected: boolean
  /** Last error message, if any */
  last_error?: string
  /** Total messages received */
  messages_received: number
  /** Seconds since the last message was received */
  seconds_since_last_message: number
  /** Circuit breaker state (closed, open, half_open) */
  circuit_breaker_state: string
  /** Consecutive failures recorded by the circuit breaker */
  circuit_breaker_failures: number
  /** Requests rejected by the circuit breaker */
  circuit_breaker_rejections: number
}

/** Aggregated system health response from /health endpoint */
export interface SystemHealth {
  /** Overall system status */
  status: 'healthy' | 'degraded' | 'unhealthy'
  /** Individual actor health reports */
  actors: ActorHealth[]
  /** Connector health reports */
  connectors: ConnectorHealth[]
  /** Server uptime in seconds */
  uptime_secs: number
  /** Timestamp of the health check */
  timestamp: string
}

/** WebSocket message for actor health updates */
export interface ActorHealthMessage {
  type: 'actor_health'
  actors: ActorHealth[]
  connectors: ConnectorHealth[]
  timestamp: string
}
