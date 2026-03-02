import api from './index'
import type { PipelineMetrics } from '@/types/pipeline'

// Direct worker API calls (bypassing coordinator)
// Used when directly connected to a worker for debugging/testing

/**
 * List pipelines on a specific worker
 */
export async function listWorkerPipelines(workerAddress: string): Promise<PipelineInfo[]> {
  const response = await api.get<PipelineInfo[]>(`/pipelines`, {
    baseURL: `http://${workerAddress}/api/v1`,
  })
  return response.data
}

interface PipelineInfo {
  id: string
  name: string
  status: string
  events_processed: number
  events_emitted: number
}

/**
 * Get pipeline metrics from a worker
 */
export async function getPipelineMetrics(
  workerAddress: string,
  pipelineId: string
): Promise<PipelineMetrics> {
  const response = await api.get<PipelineMetrics>(
    `/pipelines/${pipelineId}/metrics`,
    { baseURL: `http://${workerAddress}/api/v1` }
  )
  return response.data
}

/**
 * Hot reload a pipeline on a worker
 */
export async function reloadPipeline(
  workerAddress: string,
  pipelineId: string,
  vplSource?: string
): Promise<void> {
  await api.post(
    `/pipelines/${pipelineId}/reload`,
    vplSource ? { vpl_source: vplSource } : {},
    { baseURL: `http://${workerAddress}/api/v1` }
  )
}

/**
 * Parse and validate VPL source
 */
export async function validateVpl(vplSource: string): Promise<ValidateResult> {
  const response = await api.post<ValidateResult>('/validate', { source: vplSource })
  return response.data
}

interface ValidateResult {
  valid: boolean
  errors?: ValidationError[]
  warnings?: ValidationWarning[]
}

interface ValidationError {
  line: number
  column: number
  message: string
}

interface ValidationWarning {
  line: number
  column: number
  message: string
}

/**
 * Get available connectors
 */
export async function listConnectors(): Promise<ConnectorInfo[]> {
  const response = await api.get<ConnectorInfo[]>('/connectors')
  return response.data
}

interface ConnectorInfo {
  name: string
  type: 'source' | 'sink' | 'both'
  description: string
  config_schema: Record<string, unknown>
}

/**
 * Test a connector configuration
 */
export async function testConnector(
  connectorType: string,
  config: Record<string, unknown>
): Promise<TestConnectorResult> {
  const response = await api.post<TestConnectorResult>('/connectors/test', {
    connector_type: connectorType,
    config,
  })
  return response.data
}

interface TestConnectorResult {
  success: boolean
  message: string
  details?: Record<string, unknown>
}

// === Topology ===

export interface TopologyNode {
  id: number
  name: string
  node_type: string
  operation_count: number
  operation_summary: string
  metrics: {
    events_received: number
    events_emitted: number
    events_filtered: number
    processing_time_ns: number
  }
}

export interface TopologyEdge {
  from: number
  to: number
  edge_type: string
  label: string
}

export interface PipelineTopology {
  nodes: TopologyNode[]
  edges: TopologyEdge[]
  sources: number[]
  sinks: number[]
}

/**
 * Get pipeline topology for visualization (proxied via coordinator)
 */
export async function getPipelineTopology(
  groupId: string
): Promise<PipelineTopology> {
  const response = await api.get<PipelineTopology>(
    `/cluster/pipeline-groups/${groupId}/topology`
  )
  return response.data
}

// === EXPLAIN ===

export interface ExplainResult {
  logical_plan: string
  optimized_plan?: string
}

/**
 * Get EXPLAIN plan for VPL source
 */
export async function explainVpl(vplSource: string): Promise<ExplainResult> {
  const response = await api.post<ExplainResult>('/explain', { source: vplSource })
  return response.data
}

// === Available Connectors (Component Registry) ===

export interface AvailableConnector {
  connector_type: string
  display_name: string
  description: string
  feature_flag: string | null
  supports_source: boolean
  supports_sink: boolean
  supports_managed: boolean
  config_params: ConfigParam[]
}

export interface ConfigParam {
  name: string
  description: string
  required: boolean
  default_value: string | null
}

/**
 * List available connector types from the component registry
 */
export async function listAvailableConnectors(): Promise<AvailableConnector[]> {
  const response = await api.get<AvailableConnector[]>('/connectors/available')
  return response.data
}
