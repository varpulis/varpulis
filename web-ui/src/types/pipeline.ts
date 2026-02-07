// Pipeline group status (matches Rust GroupStatus)
export type PipelineGroupStatus = 'deploying' | 'running' | 'partially_running' | 'failed' | 'torn_down'

// Individual pipeline status
export type PipelineStatus = 'Deploying' | 'Running' | 'Failed' | 'Stopped'

// Pipeline group from coordinator (matches Rust PipelineGroupInfo)
export interface PipelineGroup {
  id: string
  name: string
  status: PipelineGroupStatus
  pipeline_count: number
  placements: PipelinePlacementInfo[]
}

// Pipeline placement info from coordinator
export interface PipelinePlacementInfo {
  pipeline_name: string
  worker_id: string
  worker_address: string
  pipeline_id: string
  status: string
}

// Individual pipeline info (for UI display purposes)
export interface PipelineInfo {
  id: string
  name: string
  worker_id: string
  worker_address: string
  status: PipelineStatus
  vpl_source?: string
  affinity?: AffinityRule
  metrics?: PipelineMetrics
}

// Pipeline metrics
export interface PipelineMetrics {
  events_in: number
  events_out: number
  events_error: number
  bytes_in: number
  bytes_out: number
  avg_latency_ms: number
  p50_latency_ms: number
  p95_latency_ms: number
  p99_latency_ms: number
  active_streams: number
}

// Route configuration
export interface RouteConfig {
  id: string
  source: string
  target: string
  event_types: string[]
  filter?: string
}

// Pipeline affinity rules
export interface AffinityRule {
  type: 'worker_id' | 'label' | 'any'
  value?: string
}

// Pipeline group specification for deployment (matches Rust PipelineGroupSpec)
export interface PipelineGroupSpec {
  name: string
  pipelines: PipelineSpec[]
  routes?: RouteSpec[]
}

// Individual pipeline specification (matches Rust PipelinePlacement)
export interface PipelineSpec {
  name: string
  source: string
  worker_affinity?: string
}

// Route specification (matches Rust InterPipelineRoute)
export interface RouteSpec {
  from_pipeline: string
  to_pipeline: string
  event_types: string[]
  mqtt_topic?: string
}

// Event injection payload
export interface EventPayload {
  event_type: string
  data: Record<string, unknown>
  timestamp?: string
}

// Event injection response
export interface InjectResponse {
  success: boolean
  routed_to: string[]
  worker_id?: string
  errors?: string[]
}

// Deploy dialog form state
export interface DeployFormState {
  name: string
  pipelines: PipelineFormEntry[]
  routes: RouteFormEntry[]
}

export interface PipelineFormEntry {
  name: string
  vpl_source: string
  affinityType: 'any' | 'worker_id' | 'label'
  affinityValue: string
}

export interface RouteFormEntry {
  source: string
  target: string
  eventTypes: string
  filter: string
}
