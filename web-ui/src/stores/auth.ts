import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'
import { useOrgStore } from './org'

export interface User {
  id: string
  name: string
  login: string
  avatar: string
  email: string
  user_id?: string
  org_id?: string
  role?: string
  auth_method?: string
}

const TOKEN_KEY = 'varpulis_token'
const AUTH_FLAG_KEY = 'varpulis_authenticated'

export const useAuthStore = defineStore('auth', () => {
  const user = ref<User | null>(null)
  const token = ref<string | null>(localStorage.getItem(TOKEN_KEY))
  const loading = ref(false)
  const error = ref<string | null>(null)

  const isAuthenticated = computed(() => !!user.value && (!!token.value || isSessionAuth.value))
  const isSessionAuth = computed(() => localStorage.getItem(AUTH_FLAG_KEY) === 'true')

  function setToken(newToken: string) {
    token.value = newToken
    localStorage.setItem(TOKEN_KEY, newToken)
    // Set default Authorization header for all future requests
    axios.defaults.headers.common['Authorization'] = `Bearer ${newToken}`
  }

  function clearToken() {
    token.value = null
    user.value = null
    localStorage.removeItem(TOKEN_KEY)
    localStorage.removeItem(AUTH_FLAG_KEY)
    delete axios.defaults.headers.common['Authorization']
  }

  async function fetchUser() {
    if (!token.value && !isSessionAuth.value) return

    loading.value = true
    error.value = null

    try {
      if (token.value) {
        axios.defaults.headers.common['Authorization'] = `Bearer ${token.value}`
      }
      const response = await axios.get('/api/v1/me', { withCredentials: true })
      user.value = response.data
      // Load organizations after user is fetched
      const orgStore = useOrgStore()
      orgStore.loadOrgs()
    } catch (err: unknown) {
      // Token is invalid or expired
      clearToken()
      if (err instanceof Error) {
        error.value = err.message
      }
    } finally {
      loading.value = false
    }
  }

  async function loginWithPassword(username: string, password: string) {
    loading.value = true
    error.value = null

    try {
      const response = await axios.post('/auth/login', { username, password }, { withCredentials: true })
      const data = response.data

      if (data.ok && data.token) {
        // Store token for Bearer header auth (backward compat)
        setToken(data.token)
        // Mark session-based auth as active
        localStorage.setItem(AUTH_FLAG_KEY, 'true')
        user.value = data.user

        // Load organizations
        const orgStore = useOrgStore()
        orgStore.loadOrgs()

        // Navigate to dashboard
        window.location.href = '/'
      } else {
        error.value = data.error || 'Login failed'
      }
    } catch (err: unknown) {
      if (axios.isAxiosError(err) && err.response?.data?.error) {
        error.value = err.response.data.error
      } else if (err instanceof Error) {
        error.value = err.message
      } else {
        error.value = 'Login failed'
      }
    } finally {
      loading.value = false
    }
  }

  async function renewSession() {
    try {
      const response = await axios.post('/auth/renew', {}, { withCredentials: true })
      if (response.data.ok && response.data.token) {
        setToken(response.data.token)
      }
    } catch {
      // Session renewal failed — clear auth
      clearToken()
    }
  }

  async function logout() {
    try {
      await axios.post('/auth/logout', {}, { withCredentials: true })
    } catch {
      // Ignore errors during logout
    }
    clearToken()
  }

  function loginWithGitHub() {
    // Redirect to backend OAuth endpoint
    window.location.href = '/auth/github'
  }

  // Check for token in URL query params (after OAuth callback redirect)
  function handleOAuthCallback() {
    const params = new URLSearchParams(window.location.search)
    const urlToken = params.get('token')
    if (urlToken) {
      setToken(urlToken)
      // Clean the URL
      const url = new URL(window.location.href)
      url.searchParams.delete('token')
      window.history.replaceState({}, '', url.pathname + url.search)
      // Fetch user profile
      fetchUser()
    } else if (token.value) {
      // Already have a stored token — validate it
      fetchUser()
    } else if (isSessionAuth.value) {
      // Cookie-based session — try to fetch user
      fetchUser()
    }
  }

  return {
    user,
    token,
    loading,
    error,
    isAuthenticated,
    isSessionAuth,
    setToken,
    clearToken,
    fetchUser,
    loginWithPassword,
    renewSession,
    logout,
    loginWithGitHub,
    handleOAuthCallback,
  }
})
