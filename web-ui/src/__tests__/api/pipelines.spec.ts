import { describe, it, expect, vi, beforeEach } from 'vitest'

// Mock the axios instance from @/api/index
const mockGet = vi.fn()
const mockPost = vi.fn()

vi.mock('@/api/index', () => ({
  default: {
    get: (...args: unknown[]) => mockGet(...args),
    post: (...args: unknown[]) => mockPost(...args),
  },
}))

import {
  listWorkerPipelines,
  getPipelineMetrics,
  reloadPipeline,
  validateVpl,
  listConnectors,
  testConnector,
} from '@/api/pipelines'

describe('pipelines API', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // --- listWorkerPipelines ---

  it('listWorkerPipelines calls GET /pipelines on the worker', async () => {
    const pipelines = [
      { id: 'p-1', name: 'main', status: 'running', events_processed: 100, events_emitted: 50 },
    ]
    mockGet.mockResolvedValue({ data: pipelines })

    const result = await listWorkerPipelines('worker1:9000')

    expect(mockGet).toHaveBeenCalledWith('/pipelines', {
      baseURL: 'http://worker1:9000/api/v1',
    })
    expect(result).toEqual(pipelines)
  })

  // --- getPipelineMetrics ---

  it('getPipelineMetrics calls GET /pipelines/:id/metrics on the worker', async () => {
    const metrics = { events_processed: 500, throughput_eps: 100 }
    mockGet.mockResolvedValue({ data: metrics })

    const result = await getPipelineMetrics('worker1:9000', 'p-1')

    expect(mockGet).toHaveBeenCalledWith('/pipelines/p-1/metrics', {
      baseURL: 'http://worker1:9000/api/v1',
    })
    expect(result).toEqual(metrics)
  })

  // --- reloadPipeline ---

  it('reloadPipeline calls POST with vpl_source when provided', async () => {
    mockPost.mockResolvedValue({})

    await reloadPipeline('worker1:9000', 'p-1', 'stream S = ...')

    expect(mockPost).toHaveBeenCalledWith(
      '/pipelines/p-1/reload',
      { vpl_source: 'stream S = ...' },
      { baseURL: 'http://worker1:9000/api/v1' }
    )
  })

  it('reloadPipeline calls POST with empty body when no vpl_source', async () => {
    mockPost.mockResolvedValue({})

    await reloadPipeline('worker1:9000', 'p-1')

    expect(mockPost).toHaveBeenCalledWith('/pipelines/p-1/reload', {}, {
      baseURL: 'http://worker1:9000/api/v1',
    })
  })

  // --- validateVpl ---

  it('validateVpl calls POST /validate with source', async () => {
    const validateResult = { valid: true }
    mockPost.mockResolvedValue({ data: validateResult })

    const result = await validateVpl('stream S = A -> B .within(1m)')

    expect(mockPost).toHaveBeenCalledWith('/validate', {
      source: 'stream S = A -> B .within(1m)',
    })
    expect(result).toEqual(validateResult)
  })

  it('validateVpl returns errors when invalid', async () => {
    const validateResult = {
      valid: false,
      errors: [{ line: 1, column: 8, message: 'unexpected token' }],
    }
    mockPost.mockResolvedValue({ data: validateResult })

    const result = await validateVpl('stream S = ???')

    expect(result.valid).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors![0].message).toBe('unexpected token')
  })

  // --- listConnectors ---

  it('listConnectors calls GET /connectors', async () => {
    const connectors = [
      { name: 'mqtt', type: 'source', description: 'MQTT', config_schema: {} },
    ]
    mockGet.mockResolvedValue({ data: connectors })

    const result = await listConnectors()

    expect(mockGet).toHaveBeenCalledWith('/connectors')
    expect(result).toEqual(connectors)
  })

  // --- testConnector ---

  it('testConnector calls POST /connectors/test', async () => {
    const testResult = { success: true, message: 'Connection established' }
    mockPost.mockResolvedValue({ data: testResult })

    const result = await testConnector('mqtt', { url: 'tcp://localhost:1883' })

    expect(mockPost).toHaveBeenCalledWith('/connectors/test', {
      connector_type: 'mqtt',
      config: { url: 'tcp://localhost:1883' },
    })
    expect(result).toEqual(testResult)
  })

  it('testConnector returns failure details', async () => {
    const testResult = {
      success: false,
      message: 'Connection refused',
      details: { error_code: 'ECONNREFUSED' },
    }
    mockPost.mockResolvedValue({ data: testResult })

    const result = await testConnector('kafka', { url: 'localhost:9092' })

    expect(result.success).toBe(false)
    expect(result.details).toEqual({ error_code: 'ECONNREFUSED' })
  })
})
