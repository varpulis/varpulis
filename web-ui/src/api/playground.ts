import axios from 'axios'

function getPlaygroundBaseUrl(): string {
  const coordinatorUrl = localStorage.getItem('varpulis_coordinator_url')
  if (coordinatorUrl && coordinatorUrl.trim()) {
    return `${coordinatorUrl.trim().replace(/\/$/, '')}/api/v1/playground`
  }
  return '/api/v1/playground'
}

const pgApi = axios.create({
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
})

export interface PlaygroundDiagnostic {
  severity: string
  message: string
  hint?: string
  code?: string
  start_line: number
  start_col: number
  end_line: number
  end_col: number
}

export interface PlaygroundRunResponse {
  ok: boolean
  events_processed: number
  output_events: Record<string, unknown>[]
  latency_ms: number
  diagnostics: PlaygroundDiagnostic[]
  error?: string
}

export interface PlaygroundValidateResponse {
  ok: boolean
  ast?: unknown
  diagnostics: PlaygroundDiagnostic[]
}

export interface PlaygroundExample {
  id: string
  name: string
  description: string
  category: string
}

export interface PlaygroundExampleDetail extends PlaygroundExample {
  vpl: string
  events: string
  expected_output_count?: number
}

export async function playgroundRun(
  vpl: string,
  events: string
): Promise<PlaygroundRunResponse> {
  const resp = await pgApi.post(`${getPlaygroundBaseUrl()}/run`, { vpl, events })
  return resp.data
}

export async function playgroundValidate(
  vpl: string
): Promise<PlaygroundValidateResponse> {
  const resp = await pgApi.post(`${getPlaygroundBaseUrl()}/validate`, { vpl })
  return resp.data
}

export async function listExamples(): Promise<PlaygroundExample[]> {
  const resp = await pgApi.get(`${getPlaygroundBaseUrl()}/examples`)
  return resp.data
}

export async function getExample(id: string): Promise<PlaygroundExampleDetail> {
  const resp = await pgApi.get(`${getPlaygroundBaseUrl()}/examples/${id}`)
  return resp.data
}
