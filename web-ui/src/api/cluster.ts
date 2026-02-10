import api from './index'
import type {
  Worker,
  WorkerDetail,
  TopologyInfo,
  ClusterSummary,
} from '@/types/cluster'
import type {
  PipelineGroup,
  PipelineGroupSpec,
  EventPayload,
  InjectResponse,
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
export async function drainWorker(id: string): Promise<void> {
  await api.post(`${CLUSTER_BASE}/workers/${id}/drain`)
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

// === Health ===

/**
 * Check coordinator health
 */
export async function checkHealth(): Promise<{ status: string; version: string }> {
  const response = await api.get<{ status: string; version: string }>('/health')
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
