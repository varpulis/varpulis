import axios, { type AxiosInstance, type AxiosError, type InternalAxiosRequestConfig } from 'axios'

// Get the base URL from settings or use default proxy path
function getBaseUrl(): string {
  const coordinatorUrl = localStorage.getItem('varpulis_coordinator_url')
  if (coordinatorUrl && coordinatorUrl.trim()) {
    // Use absolute URL if coordinator URL is set
    return `${coordinatorUrl.trim().replace(/\/$/, '')}/api/v1`
  }
  // Default: use Vite proxy
  return '/api/v1'
}

// Create axios instance with default configuration
const api: AxiosInstance = axios.create({
  baseURL: getBaseUrl(),
  timeout: 30000,
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor for adding auth headers and updating baseURL
api.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    // Update baseURL in case it changed
    config.baseURL = getBaseUrl()

    // Add JWT Bearer token if available (set by auth store after login)
    const token = localStorage.getItem('varpulis_token')
    if (token) {
      config.headers['Authorization'] = `Bearer ${token}`
    }

    // Get API key from sessionStorage (not persisted across sessions for security)
    const apiKey = sessionStorage.getItem('varpulis_api_key')
    if (apiKey) {
      config.headers['X-API-Key'] = apiKey
    }
    return config
  },
  (error: AxiosError) => {
    return Promise.reject(error)
  }
)

// Track renewal attempts to prevent infinite loops
let isRenewing = false

// Response interceptor for error handling and session renewal
api.interceptors.response.use(
  (response) => response,
  async (error: AxiosError) => {
    if (error.response) {
      const status = error.response.status
      const data = error.response.data as { error?: string; message?: string }
      const originalRequest = error.config

      // Auto-renew session on 401 (but not for login/renew endpoints)
      if (status === 401 && originalRequest && !isRenewing) {
        const url = originalRequest.url || ''
        if (!url.includes('/auth/login') && !url.includes('/auth/renew') && !url.includes('/auth/logout')) {
          isRenewing = true
          try {
            const renewResponse = await axios.post('/auth/renew', {}, { withCredentials: true })
            if (renewResponse.data?.ok && renewResponse.data?.token) {
              // Update token in localStorage
              localStorage.setItem('varpulis_token', renewResponse.data.token)
              axios.defaults.headers.common['Authorization'] = `Bearer ${renewResponse.data.token}`
              // Retry original request
              if (originalRequest.headers) {
                originalRequest.headers['Authorization'] = `Bearer ${renewResponse.data.token}`
              }
              isRenewing = false
              return axios(originalRequest)
            }
          } catch {
            // Renewal failed — clear auth and redirect to login
          }
          isRenewing = false

          // Clear auth state and redirect to login
          localStorage.removeItem('varpulis_token')
          localStorage.removeItem('varpulis_authenticated')
          delete axios.defaults.headers.common['Authorization']
          if (window.location.pathname !== '/login') {
            window.location.href = '/login?redirect=' + encodeURIComponent(window.location.pathname)
          }
        }
      }

      switch (status) {
        case 401:
          // Handled above — don't log for every failed health check
          break
        case 403:
          console.error('Access forbidden.')
          break
        case 404:
          console.error('Resource not found:', error.config?.url)
          break
        case 500:
          console.error('Server error:', data?.error || data?.message || 'Unknown error')
          break
        default:
          console.error(`API error (${status}):`, data?.error || data?.message)
      }
    } else if (error.request) {
      console.error('Network error: No response received from server')
    } else {
      console.error('Request error:', error.message)
    }
    return Promise.reject(error)
  }
)

// Set API key helper
export function setApiKey(key: string): void {
  sessionStorage.setItem('varpulis_api_key', key)
}

// Clear API key helper
export function clearApiKey(): void {
  sessionStorage.removeItem('varpulis_api_key')
}

// Get current API key — checks sessionStorage first, then falls back to localStorage if "remember" was enabled
export function getApiKey(): string | null {
  const sessionKey = sessionStorage.getItem('varpulis_api_key')
  if (sessionKey) return sessionKey
  // Fall back: restore from localStorage if rememberApiKey was enabled
  try {
    const stored = localStorage.getItem('varpulis_settings')
    if (stored) {
      const settings = JSON.parse(stored)
      if (settings.rememberApiKey && settings.apiKey) {
        // Re-hydrate into sessionStorage for the current session
        sessionStorage.setItem('varpulis_api_key', settings.apiKey)
        return settings.apiKey
      }
    }
  } catch {
    // ignore
  }
  return null
}

export default api
