import api from './index'
import type {
  Worker,
  WorkerDetail,
  TopologyInfo,
  ClusterSummary,
  Migration,
  RaftClusterStatus,
} from '@/types/cluster'
import type {
  PipelineGroup,
  PipelineGroupSpec,
  EventPayload,
  InjectResponse,
  InjectBatchResponse,
} from '@/types/pipeline'

const CLUSTER_BASE = '/cluster'

// === Worker Management ===

interface WorkersResponse {
  total: number
  workers: Worker[]
}

/**
 * List all registered workers
 */
export async function listWorkers(): Promise<Worker[]> {
  const response = await api.get<WorkersResponse>(`${CLUSTER_BASE}/workers`)
  return response.data.workers
}

/**
 * Get details for a specific worker
 */
export async function getWorker(id: string): Promise<WorkerDetail> {
  const response = await api.get<WorkerDetail>(`${CLUSTER_BASE}/workers/${id}`)
  return response.data
}

/**
 * Remove a worker from the cluster
 */
export async function deleteWorker(id: string): Promise<void> {
  await api.delete(`${CLUSTER_BASE}/workers/${id}`)
}

/**
 * Drain a worker (stop accepting new pipelines, migrate existing)
 */
export async function drainWorker(id: string, timeoutSecs?: number): Promise<{ worker_id: string; pipelines_migrated: number; status: string }> {
  const response = await api.post(`${CLUSTER_BASE}/workers/${id}/drain`, { timeout_secs: timeoutSecs ?? null })
  return response.data
}

// === Pipeline Groups ===

interface PipelineGroupsResponse {
  total: number
  pipeline_groups: PipelineGroup[]
}

/**
 * List all pipeline groups
 */
export async function listPipelineGroups(): Promise<PipelineGroup[]> {
  const response = await api.get<PipelineGroupsResponse>(`${CLUSTER_BASE}/pipeline-groups`)
  return response.data.pipeline_groups
}

/**
 * Get details for a specific pipeline group
 */
export async function getPipelineGroup(id: string): Promise<PipelineGroup> {
  const response = await api.get<PipelineGroup>(`${CLUSTER_BASE}/pipeline-groups/${id}`)
  return response.data
}

/**
 * Deploy a new pipeline group
 */
export async function deployPipelineGroup(spec: PipelineGroupSpec): Promise<PipelineGroup> {
  const response = await api.post<PipelineGroup>(`${CLUSTER_BASE}/pipeline-groups`, spec)
  return response.data
}

/**
 * Update an existing pipeline group
 */
export async function updatePipelineGroup(id: string, spec: Partial<PipelineGroupSpec>): Promise<PipelineGroup> {
  const response = await api.patch<PipelineGroup>(`${CLUSTER_BASE}/pipeline-groups/${id}`, spec)
  return response.data
}

/**
 * Teardown (delete) a pipeline group
 */
export async function teardownGroup(id: string): Promise<void> {
  await api.delete(`${CLUSTER_BASE}/pipeline-groups/${id}`)
}

/**
 * Inject an event into a pipeline group
 */
export async function injectEvent(groupId: string, event: EventPayload): Promise<InjectResponse> {
  const response = await api.post<InjectResponse>(
    `${CLUSTER_BASE}/pipeline-groups/${groupId}/inject`,
    event
  )
  return response.data
}

/**
 * Inject a batch of events (in .evt text format) into a pipeline group
 */
export async function injectBatch(groupId: string, eventsText: string): Promise<InjectBatchResponse> {
  const response = await api.post<InjectBatchResponse>(
    `${CLUSTER_BASE}/pipeline-groups/${groupId}/inject-batch`,
    { events_text: eventsText }
  )
  return response.data
}

// === Topology ===

/**
 * Get cluster topology information
 */
export async function getTopology(): Promise<TopologyInfo> {
  const response = await api.get<TopologyInfo>(`${CLUSTER_BASE}/topology`)
  return response.data
}

// === Cluster Summary ===

/**
 * Get cluster summary statistics
 */
export async function getClusterSummary(): Promise<ClusterSummary> {
  const response = await api.get<ClusterSummary>(`${CLUSTER_BASE}/summary`)
  return response.data
}

// === Connectors ===

export interface ClusterConnector {
  name: string
  connector_type: string
  params: Record<string, string>
  description?: string
}

interface ConnectorsResponse {
  total: number
  connectors: ClusterConnector[]
}

/**
 * List all cluster connectors
 */
export async function listConnectors(): Promise<ClusterConnector[]> {
  const response = await api.get<ConnectorsResponse>(`${CLUSTER_BASE}/connectors`)
  return response.data.connectors
}

/**
 * Get a specific connector
 */
export async function getConnector(name: string): Promise<ClusterConnector> {
  const response = await api.get<ClusterConnector>(`${CLUSTER_BASE}/connectors/${name}`)
  return response.data
}

/**
 * Create a new connector
 */
export async function createConnector(connector: ClusterConnector): Promise<ClusterConnector> {
  const response = await api.post<ClusterConnector>(`${CLUSTER_BASE}/connectors`, connector)
  return response.data
}

/**
 * Update an existing connector
 */
export async function updateConnector(name: string, connector: ClusterConnector): Promise<ClusterConnector> {
  const response = await api.put<ClusterConnector>(`${CLUSTER_BASE}/connectors/${name}`, connector)
  return response.data
}

/**
 * Delete a connector
 */
export async function deleteConnector(name: string): Promise<void> {
  await api.delete(`${CLUSTER_BASE}/connectors/${name}`)
}

// === Metrics ===

export interface PipelineWorkerMetrics {
  pipeline_name: string
  worker_id: string
  events_in: number
  events_out: number
}

export interface ClusterMetricsResponse {
  pipelines: PipelineWorkerMetrics[]
}

/**
 * Fetch real-time cluster metrics
 */
export async function fetchClusterMetrics(): Promise<ClusterMetricsResponse> {
  const response = await api.get<ClusterMetricsResponse>(`${CLUSTER_BASE}/metrics`)
  return response.data
}

// === Migrations ===

/**
 * List all active/recent migrations
 */
export async function listMigrations(): Promise<Migration[]> {
  const response = await api.get<{ migrations: Migration[]; total: number }>(`${CLUSTER_BASE}/migrations`)
  return response.data.migrations
}

/**
 * Get details for a specific migration
 */
export async function getMigration(id: string): Promise<Migration> {
  const response = await api.get<Migration>(`${CLUSTER_BASE}/migrations/${id}`)
  return response.data
}

/**
 * Trigger cluster rebalancing
 */
export async function triggerRebalance(): Promise<{ migrations_started: number; migration_ids: string[] }> {
  const response = await api.post<{ migrations_started: number; migration_ids: string[] }>(`${CLUSTER_BASE}/rebalance`)
  return response.data
}

/**
 * Manually migrate a pipeline to a target worker
 */
export async function migratePipeline(groupId: string, pipelineName: string, targetWorkerId: string): Promise<{ migration_id: string }> {
  const response = await api.post<{ migration_id: string }>(`${CLUSTER_BASE}/pipelines/${groupId}/${pipelineName}/migrate`, { target_worker_id: targetWorkerId })
  return response.data
}

// === Prometheus Metrics ===

import type { ClusterHealthMetrics } from '@/types/cluster'

/**
 * Parse Prometheus text format into key-value pairs
 */
function parsePrometheusText(text: string): Map<string, number> {
  const metrics = new Map<string, number>()
  for (const line of text.split('\n')) {
    if (line.startsWith('#') || line.trim() === '') continue
    // Match: metric_name{labels} value or metric_name value
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([\d.eE+-]+|NaN|Inf|-Inf)/)
    if (match) {
      const key = match[1] + (match[2] || '')
      const value = parseFloat(match[3])
      if (!isNaN(value)) {
        metrics.set(key, value)
      }
    }
  }
  return metrics
}

/**
 * Fetch cluster Prometheus metrics and parse into structured format
 */
export async function fetchClusterHealth(): Promise<ClusterHealthMetrics> {
  const response = await api.get(`${CLUSTER_BASE}/prometheus`, {
    headers: { Accept: 'text/plain' },
    transformResponse: [(data: string) => data], // prevent JSON parse
  })
  const metrics = parsePrometheusText(response.data)

  return {
    raft_role: metrics.get('varpulis_cluster_raft_role') ?? -1,
    raft_term: metrics.get('varpulis_cluster_raft_term') ?? 0,
    raft_commit_index: metrics.get('varpulis_cluster_raft_commit_index') ?? 0,
    workers_ready: metrics.get('varpulis_cluster_workers_total{status="ready"}') ?? 0,
    workers_unhealthy: metrics.get('varpulis_cluster_workers_total{status="unhealthy"}') ?? 0,
    workers_draining: metrics.get('varpulis_cluster_workers_total{status="draining"}') ?? 0,
    pipeline_groups_total: metrics.get('varpulis_cluster_pipeline_groups_total') ?? 0,
    deployments_total: metrics.get('varpulis_cluster_deployments_total') ?? 0,
    migrations_success: metrics.get('varpulis_cluster_migrations_total{result="success"}') ?? 0,
    migrations_failure: metrics.get('varpulis_cluster_migrations_total{result="failure"}') ?? 0,
    deploys_success: metrics.get('varpulis_cluster_deploy_duration_seconds_count{result="success"}') ?? 0,
    deploys_failure: metrics.get('varpulis_cluster_deploy_duration_seconds_count{result="failure"}') ?? 0,
  }
}

// === Raft Cluster Status ===

export async function getRaftStatus(): Promise<RaftClusterStatus> {
  const response = await api.get<RaftClusterStatus>(`${CLUSTER_BASE}/raft`)
  return response.data
}

// === Validation ===

export interface ValidateDiagnostic {
  severity: 'error' | 'warning' | 'info'
  line: number
  column: number
  message: string
  hint?: string
}

export interface ValidateResponse {
  valid: boolean
  diagnostics: ValidateDiagnostic[]
}

/**
 * Validate VPL source code
 */
export async function validateVpl(source: string): Promise<ValidateResponse> {
  const response = await api.post<ValidateResponse>(`${CLUSTER_BASE}/validate`, { source })
  return response.data
}

// === Model Registry ===

import type { ModelRegistryEntry, ChatMessage, ChatResponse, LlmConfigResponse } from '@/types/cluster'

/**
 * List all registered models
 */
export async function listModels(): Promise<ModelRegistryEntry[]> {
  const response = await api.get<{ models: ModelRegistryEntry[]; total: number }>(`${CLUSTER_BASE}/models`)
  return response.data.models
}

/**
 * Upload a new model to the registry
 */
export async function uploadModel(data: {
  name: string
  inputs: string[]
  outputs: string[]
  description?: string
  data_base64?: string
}): Promise<ModelRegistryEntry> {
  const response = await api.post<ModelRegistryEntry>(`${CLUSTER_BASE}/models`, data)
  return response.data
}

/**
 * Delete a model from the registry
 */
export async function deleteModel(name: string): Promise<void> {
  await api.delete(`${CLUSTER_BASE}/models/${name}`)
}

// === AI Chat ===

/**
 * Send a chat message to the AI assistant
 */
export async function sendChatMessage(messages: ChatMessage[]): Promise<ChatResponse> {
  const response = await api.post<ChatResponse>(`${CLUSTER_BASE}/chat`, { messages })
  return response.data
}

/**
 * Get current LLM configuration
 */
export async function getChatConfig(): Promise<LlmConfigResponse> {
  const response = await api.get<LlmConfigResponse>(`${CLUSTER_BASE}/chat/config`)
  return response.data
}

/**
 * Update LLM configuration
 */
export async function updateChatConfig(config: {
  endpoint: string
  model: string
  api_key?: string
  provider: string
}): Promise<void> {
  await api.put(`${CLUSTER_BASE}/chat/config`, config)
}
