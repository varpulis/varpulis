import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export interface User {
  id: string
  name: string
  login: string
  avatar: string
  email: string
}

const TOKEN_KEY = 'varpulis_token'

export const useAuthStore = defineStore('auth', () => {
  const user = ref<User | null>(null)
  const token = ref<string | null>(localStorage.getItem(TOKEN_KEY))
  const loading = ref(false)
  const error = ref<string | null>(null)

  const isAuthenticated = computed(() => !!user.value && !!token.value)

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
    delete axios.defaults.headers.common['Authorization']
  }

  async function fetchUser() {
    if (!token.value) return

    loading.value = true
    error.value = null

    try {
      axios.defaults.headers.common['Authorization'] = `Bearer ${token.value}`
      const response = await axios.get('/api/v1/me')
      user.value = response.data
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

  async function logout() {
    try {
      await axios.post('/auth/logout')
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
    }
  }

  return {
    user,
    token,
    loading,
    error,
    isAuthenticated,
    setToken,
    clearToken,
    fetchUser,
    logout,
    loginWithGitHub,
    handleOAuthCallback,
  }
})
