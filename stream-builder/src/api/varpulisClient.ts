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

/** Validate VPL syntax via the server */
export async function validateVpl(source: string): Promise<{ valid: boolean; error?: string; diagnostics?: unknown[] }> {
  const result = await apiCall('POST', '/api/v1/playground/validate', { vpl: source })
  if (result.ok) {
    const data = result.data as { ok: boolean; diagnostics?: unknown[] }
    if (data.ok) {
      return { valid: true, diagnostics: data.diagnostics }
    }
    return { valid: false, error: 'Validation returned errors' }
  }
  // Parse error from server
  const data = result.data as { error?: string; code?: string } | null
  return { valid: false, error: data?.error || result.error }
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
export async function listPipelines(): Promise<{ pipelines: Array<{ id: string; name: string; status: string }>; error?: string }> {
  const result = await apiCall('GET', '/api/v1/pipelines')
  if (result.ok && result.data) {
    const data = result.data as { pipelines: Array<{ id: string; name: string; status: string }> }
    return { pipelines: data.pipelines }
  }
  return { pipelines: [], error: result.error }
}
