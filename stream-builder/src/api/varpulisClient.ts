// Server URL: env var, localStorage override, or same-origin (nginx proxy).
// When served via nginx proxy, use '' (empty = same origin, all /api/ calls proxied).
const DEFAULT_SERVER = import.meta.env.VITE_VARPULIS_URL || ''
const DEFAULT_API_KEY = import.meta.env.VITE_VARPULIS_API_KEY || ''

function getServerUrl(): string {
  return localStorage.getItem('varpulis_server_url') || DEFAULT_SERVER
}

function getApiKey(): string {
  return localStorage.getItem('varpulis_api_key') || DEFAULT_API_KEY
}

export function setServerConfig(url: string, apiKey: string) {
  localStorage.setItem('varpulis_server_url', url)
  localStorage.setItem('varpulis_api_key', apiKey)
}

export function getServerConfig() {
  return { url: getServerUrl(), apiKey: getApiKey() }
}

async function apiCall(method: string, path: string, body?: unknown): Promise<{ ok: boolean; data?: unknown; error?: string }> {
  const url = `${getServerUrl()}${path}`
  try {
    const resp = await fetch(url, {
      method,
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': getApiKey(),
      },
      body: body ? JSON.stringify(body) : undefined,
    })
    const data = await resp.json().catch(() => null)
    if (resp.ok) {
      return { ok: true, data }
    }
    const errMsg = data?.message || data?.error || `HTTP ${resp.status}`
    return { ok: false, data, error: errMsg as string }
  } catch (e) {
    return { ok: false, error: `Connection failed: ${e instanceof Error ? e.message : 'unknown'}` }
  }
}

/** A single diagnostic from server validation */
export interface VplDiagnostic {
  severity: 'error' | 'warning' | 'info' | 'hint'
  message: string
  hint?: string
  code?: string
  start_line: number
  start_col: number
  end_line: number
  end_col: number
}

/** Validate VPL syntax via the server */
export async function validateVpl(source: string): Promise<{ valid: boolean; error?: string; diagnostics: VplDiagnostic[] }> {
  const result = await apiCall('POST', '/api/v1/playground/validate', { vpl: source })
  if (result.ok) {
    const data = result.data as { ok: boolean; diagnostics?: VplDiagnostic[] }
    const diags = (data.diagnostics ?? []) as VplDiagnostic[]
    if (data.ok && diags.every((d) => d.severity !== 'error')) {
      return { valid: true, diagnostics: diags }
    }
    const errors = diags.filter((d) => d.severity === 'error')
    return { valid: false, error: errors[0]?.message || 'Validation returned errors', diagnostics: diags }
  }
  const data = result.data as { error?: string; code?: string } | null
  const errMsg = data?.error || result.error || 'Unknown error'
  return {
    valid: false,
    error: errMsg,
    diagnostics: [{ severity: 'error', message: errMsg, start_line: 1, start_col: 0, end_line: 1, end_col: 0 }],
  }
}

/** Cluster-aware validation: tries /cluster/validate then falls back to /playground/validate */
export async function validateVplServer(source: string): Promise<{ valid: boolean; error?: string; diagnostics: VplDiagnostic[] }> {
  // Try cluster endpoint first (uses injected connectors for better validation)
  const clusterResult = await apiCall('POST', '/api/v1/cluster/validate', { source })
  if (clusterResult.ok) {
    const data = clusterResult.data as { valid: boolean; diagnostics?: Array<{ severity: string; line: number; column: number; message: string; hint?: string }> }
    const diags: VplDiagnostic[] = (data.diagnostics ?? []).map((d) => ({
      severity: d.severity as VplDiagnostic['severity'],
      message: d.message,
      hint: d.hint,
      start_line: d.line,
      start_col: Math.max(0, d.column - 1),
      end_line: d.line,
      end_col: d.column + 10, // approximate end column
    }))
    if (data.valid) {
      return { valid: true, diagnostics: diags }
    }
    const errors = diags.filter((d) => d.severity === 'error')
    return { valid: false, error: errors[0]?.message || 'Validation returned errors', diagnostics: diags }
  }

  // Fall back to playground endpoint
  return validateVpl(source)
}

/** Convert VPL code to a visual graph via the server (authoritative parse) */
export async function parseVplToGraph(vpl: string): Promise<{ ok: boolean; graph?: { nodes: GraphNode[]; edges: GraphEdge[] }; error?: string }> {
  const result = await apiCall('POST', '/api/v1/pipeline/graph', { vpl })
  if (result.ok && result.data) {
    return { ok: true, graph: result.data as { nodes: GraphNode[]; edges: GraphEdge[] } }
  }
  return { ok: false, error: result.error }
}

/** Convert a visual graph back to VPL code via the server */
export async function generateVplFromGraph(graph: { nodes: GraphNode[]; edges: GraphEdge[] }): Promise<{ ok: boolean; vpl?: string; error?: string }> {
  const result = await apiCall('POST', '/api/v1/pipeline/generate', graph)
  if (result.ok && result.data) {
    const data = result.data as { vpl: string }
    return { ok: true, vpl: data.vpl }
  }
  return { ok: false, error: result.error }
}

export interface GraphNode {
  id: string
  label: string
  node_type: string
  config: Record<string, unknown>
  position?: [number, number] | null
}

export interface GraphEdge {
  id: string
  source: string
  target: string
}

/** Deploy a pipeline to the server */
export async function deployPipeline(name: string, source: string): Promise<{ ok: boolean; id?: string; error?: string }> {
  const result = await apiCall('POST', '/api/v1/pipelines', { name, source })
  if (result.ok && result.data) {
    const data = result.data as { id: string; status: string }
    return { ok: true, id: data.id }
  }
  return { ok: false, error: result.error }
}

/** Check server health */
export async function checkHealth(): Promise<{ healthy: boolean; version?: string; error?: string }> {
  const result = await apiCall('GET', '/health')
  if (result.ok && result.data) {
    const data = result.data as { status: string; version: string }
    return { healthy: data.status === 'healthy', version: data.version }
  }
  return { healthy: false, error: result.error }
}

/** List deployed pipelines */
export async function listPipelines(): Promise<{ pipelines: Array<{ id: string; name: string; status: string; source?: string }>; error?: string }> {
  const result = await apiCall('GET', '/api/v1/pipelines')
  if (result.ok && result.data) {
    const data = result.data as { pipelines: Array<{ id: string; name: string; status: string; source?: string }> }
    return { pipelines: data.pipelines }
  }
  return { pipelines: [], error: result.error }
}

/** Get a single pipeline by ID */
export async function getPipeline(id: string): Promise<{ ok: boolean; pipeline?: { id: string; name: string; status: string; source: string }; error?: string }> {
  const result = await apiCall('GET', `/api/v1/pipelines/${encodeURIComponent(id)}`)
  if (result.ok && result.data) {
    return { ok: true, pipeline: result.data as { id: string; name: string; status: string; source: string } }
  }
  return { ok: false, error: result.error }
}
