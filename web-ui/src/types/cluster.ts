// Worker status enum matching Rust coordinator (lowercase from API)
export type WorkerStatus = 'ready' | 'unhealthy' | 'draining'

// Worker information from coordinator
export interface Worker {
  id: string
  address: string
  status: WorkerStatus
  pipelines_running: number
  max_pipelines: number
  assigned_pipelines: string[]
  last_heartbeat?: string // ISO 8601 timestamp (optional)
  registered_at?: string // ISO 8601 timestamp (optional)
  capabilities?: WorkerCapabilities
  metadata?: Record<string, string>
}

export interface WorkerCapabilities {
  max_pipelines: number
  supported_connectors: string[]
  cpu_cores: number
  memory_mb: number
}

// Worker detail response
export interface WorkerDetail extends Worker {
  pipelines?: PipelinePlacement[]
  metrics_snapshot?: WorkerMetricsSnapshot
}

export interface PipelinePlacement {
  pipeline_id: string
  pipeline_name: string
  group_id: string
  status: string
  started_at: string
}

export interface WorkerMetricsSnapshot {
  events_processed: number
  events_emitted: number
  errors: number
  avg_latency_ms: number
  uptime_secs: number
}

// Topology information
export interface TopologyInfo {
  workers: TopologyWorker[]
  routes: TopologyRoute[]
}

export interface TopologyWorker {
  id: string
  address: string
  status: WorkerStatus
  pipeline_groups: string[]
}

export interface TopologyRoute {
  source_worker: string
  source_pipeline: string
  target_worker: string
  target_pipeline: string
  route_type: string
}

// Alert types
export interface Alert {
  id: string
  severity: 'info' | 'warning' | 'error' | 'critical'
  title: string
  message: string
  timestamp: string
  source: string
  acknowledged: boolean
}

// Migration types
export type MigrationStatus = 'checkpointing' | 'deploying' | 'restoring' | 'switching' | 'cleaning_up' | 'completed' | 'failed'
export type MigrationReason = 'failover' | 'rebalance' | 'drain' | 'manual'

export interface Migration {
  id: string
  pipeline_name: string
  group_id: string
  source_worker: string
  target_worker: string
  status: string
  reason: string
  elapsed_ms: number
}

// Cluster health metrics from Prometheus endpoint
export interface ClusterHealthMetrics {
  raft_role: number // 0=follower, 1=candidate, 2=leader
  raft_term: number
  raft_commit_index: number
  workers_ready: number
  workers_unhealthy: number
  workers_draining: number
  pipeline_groups_total: number
  deployments_total: number
  migrations_success: number
  migrations_failure: number
  deploys_success: number
  deploys_failure: number
}

// Cluster summary for dashboard
export interface ClusterSummary {
  total_workers: number
  healthy_workers: number
  unhealthy_workers: number
  draining_workers: number
  total_pipeline_groups: number
  active_pipeline_groups: number
  total_events_processed: number
  events_per_second: number
}
