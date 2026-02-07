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
