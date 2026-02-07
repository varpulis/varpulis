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
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor for adding auth headers and updating baseURL
api.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    // Update baseURL in case it changed
    config.baseURL = getBaseUrl()

    // Get API key from localStorage
    const apiKey = localStorage.getItem('varpulis_api_key')
    if (apiKey) {
      config.headers['X-API-Key'] = apiKey
    }
    return config
  },
  (error: AxiosError) => {
    return Promise.reject(error)
  }
)

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response) {
      const status = error.response.status
      const data = error.response.data as { error?: string; message?: string }

      switch (status) {
        case 401:
          console.error('Authentication failed. Please check your API key.')
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
  localStorage.setItem('varpulis_api_key', key)
}

// Clear API key helper
export function clearApiKey(): void {
  localStorage.removeItem('varpulis_api_key')
}

// Get current API key
export function getApiKey(): string | null {
  return localStorage.getItem('varpulis_api_key')
}

export default api
