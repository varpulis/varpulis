<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useSettingsStore } from '@/stores/settings'
import { useWebSocketStore } from '@/stores/websocket'
import { useOrgStore } from '@/stores/org'
import { useTheme } from 'vuetify'
import { setApiKey, clearApiKey, getApiKey } from '@/api'
import api from '@/api'

const settingsStore = useSettingsStore()
const wsStore = useWebSocketStore()
const orgStore = useOrgStore()
const theme = useTheme()

const hasJwtAuth = computed(() => !!localStorage.getItem('varpulis_token'))

// ---- API Key Management ----
interface OrgApiKey {
  id: string
  name: string
  key_prefix: string
  scopes: string
  created_at: string
  expires_at: string | null
}

const orgApiKeys = ref<OrgApiKey[]>([])
const orgKeysLoading = ref(false)
const newKeyName = ref('')
const newKeyScopes = ref('*')
const newKeyExpiresIn = ref('')
const newKeyCreating = ref(false)
const newKeyResult = ref<string | null>(null)
const showNewKey = ref(false)
const revokeConfirmId = ref<string | null>(null)

const expiryOptions = [
  { title: 'Never', value: '' },
  { title: '30 days', value: '30d' },
  { title: '90 days', value: '90d' },
  { title: '1 year', value: '365d' },
]

async function loadOrgApiKeys() {
  if (!orgStore.currentOrg) return
  orgKeysLoading.value = true
  try {
    const res = await api.get(`/orgs/${orgStore.currentOrg.id}/api-keys`)
    orgApiKeys.value = res.data.api_keys ?? []
  } catch {
    orgApiKeys.value = []
  } finally {
    orgKeysLoading.value = false
  }
}

async function createOrgApiKey() {
  if (!orgStore.currentOrg || !newKeyName.value.trim()) return
  newKeyCreating.value = true
  try {
    const body: Record<string, string> = { name: newKeyName.value.trim(), scopes: newKeyScopes.value }
    if (newKeyExpiresIn.value) body.expires_in = newKeyExpiresIn.value
    const res = await api.post(`/orgs/${orgStore.currentOrg.id}/api-keys`, body)
    newKeyResult.value = res.data.api_key
    showNewKey.value = true
    newKeyName.value = ''
    newKeyScopes.value = '*'
    newKeyExpiresIn.value = ''
    await loadOrgApiKeys()
  } catch (e) {
    console.error('Failed to create API key', e)
  } finally {
    newKeyCreating.value = false
  }
}

async function revokeOrgApiKey(keyId: string) {
  if (!orgStore.currentOrg) return
  try {
    await api.delete(`/orgs/${orgStore.currentOrg.id}/api-keys/${keyId}`)
    await loadOrgApiKeys()
  } catch (e) {
    console.error('Failed to revoke API key', e)
  } finally {
    revokeConfirmId.value = null
  }
}

// ---- Team Members ----
interface OrgMember {
  user_id: string
  name: string
  email: string
  role: string
  status: string
  accepted_at: string | null
}

const orgMembers = ref<OrgMember[]>([])
const membersLoading = ref(false)
const inviteEmail = ref('')
const inviteRole = ref('member')
const inviting = ref(false)
const removeMemberConfirmId = ref<string | null>(null)

const roleOptions = [
  { title: 'Viewer', value: 'viewer' },
  { title: 'Member', value: 'member' },
  { title: 'Admin', value: 'admin' },
]

const canManageMembers = computed(() => {
  const org = orgStore.currentOrg
  return org && (org.role === 'owner' || org.role === 'admin')
})

async function loadOrgMembers() {
  if (!orgStore.currentOrg) return
  membersLoading.value = true
  try {
    const res = await api.get(`/orgs/${orgStore.currentOrg.id}/members`)
    orgMembers.value = res.data.members ?? []
  } catch {
    orgMembers.value = []
  } finally {
    membersLoading.value = false
  }
}

async function inviteMember() {
  if (!orgStore.currentOrg || !inviteEmail.value.trim()) return
  inviting.value = true
  try {
    await api.post(`/orgs/${orgStore.currentOrg.id}/members`, {
      email: inviteEmail.value.trim(),
      role: inviteRole.value,
    })
    inviteEmail.value = ''
    inviteRole.value = 'member'
    await loadOrgMembers()
  } catch (e) {
    console.error('Failed to invite member', e)
  } finally {
    inviting.value = false
  }
}

async function removeMember(userId: string) {
  if (!orgStore.currentOrg) return
  try {
    await api.delete(`/orgs/${orgStore.currentOrg.id}/members/${userId}`)
    await loadOrgMembers()
  } catch (e) {
    console.error('Failed to remove member', e)
  } finally {
    removeMemberConfirmId.value = null
  }
}

async function changeMemberRole(userId: string, newRole: string) {
  if (!orgStore.currentOrg) return
  try {
    await api.put(`/orgs/${orgStore.currentOrg.id}/members/${userId}`, { role: newRole })
    await loadOrgMembers()
  } catch (e) {
    console.error('Failed to change role', e)
  }
}

const showApiKey = ref(false)
const importDialog = ref(false)
const importJson = ref('')
const importError = ref<string | null>(null)

// Connection form state (local copy for editing)
const connectionForm = ref({
  coordinatorUrl: '',
  apiKey: '',
})
const connectionSaving = ref(false)
const connectionSaved = ref(false)
const connectionError = ref<string | null>(null)

// Initialize connection form from current settings
onMounted(() => {
  connectionForm.value.coordinatorUrl = localStorage.getItem('varpulis_coordinator_url') || settingsStore.coordinatorUrl || ''
  connectionForm.value.apiKey = getApiKey() || settingsStore.apiKey || ''
})

const connectionTesting = ref(false)

// Test connection without permanently saving
async function testConnection(): Promise<void> {
  connectionTesting.value = true
  connectionError.value = null
  connectionSaved.value = false

  // Save current values to restore if test fails
  const oldUrl = localStorage.getItem('varpulis_coordinator_url')
  const oldKey = sessionStorage.getItem('varpulis_api_key')

  try {
    // Temporarily apply test values
    const testUrl = connectionForm.value.coordinatorUrl.trim()
    if (testUrl) {
      localStorage.setItem('varpulis_coordinator_url', testUrl)
    } else {
      localStorage.removeItem('varpulis_coordinator_url')
    }

    const testApiKey = connectionForm.value.apiKey.trim()
    if (testApiKey) {
      setApiKey(testApiKey)
    } else {
      clearApiKey()
    }

    const connected = await wsStore.checkApiHealth()
    if (connected) {
      connectionSaved.value = true
      // Keep the new values since they work
    } else {
      connectionError.value = 'Could not connect to coordinator. Please check the URL and API key.'
      // Restore old values since test failed
      if (oldUrl) {
        localStorage.setItem('varpulis_coordinator_url', oldUrl)
      } else {
        localStorage.removeItem('varpulis_coordinator_url')
      }
      if (oldKey) {
        setApiKey(oldKey)
      } else {
        clearApiKey()
      }
    }
  } catch (e) {
    connectionError.value = e instanceof Error ? e.message : 'Connection test failed'
    // Restore old values on error
    if (oldUrl) {
      localStorage.setItem('varpulis_coordinator_url', oldUrl)
    }
    if (oldKey) {
      setApiKey(oldKey)
    }
  } finally {
    connectionTesting.value = false
  }
}

// Save coordinator URL to localStorage for API client
function setCoordinatorUrl(url: string): void {
  if (url && url.trim()) {
    localStorage.setItem('varpulis_coordinator_url', url.trim())
  } else {
    localStorage.removeItem('varpulis_coordinator_url')
  }
}

// Save connection settings and test connection
async function saveConnectionSettings(): Promise<void> {
  connectionSaving.value = true
  connectionError.value = null
  connectionSaved.value = false

  try {
    // Update coordinator URL (save to localStorage for API client)
    const newUrl = connectionForm.value.coordinatorUrl.trim()
    setCoordinatorUrl(newUrl)
    settingsStore.updateSetting('coordinatorUrl', newUrl)

    // Update API key
    const newApiKey = connectionForm.value.apiKey.trim()
    if (newApiKey) {
      setApiKey(newApiKey)
      settingsStore.updateSetting('apiKey', newApiKey)
    } else {
      clearApiKey()
      settingsStore.updateSetting('apiKey', '')
    }

    // Test the connection with new settings
    const connected = await wsStore.checkApiHealth()
    if (connected) {
      connectionSaved.value = true
      // Start health check with new settings
      wsStore.startHealthCheck(10000)
    } else {
      connectionError.value = 'Could not connect to coordinator. Please check the URL and API key.'
    }
  } catch (e) {
    connectionError.value = e instanceof Error ? e.message : 'Failed to save settings'
  } finally {
    connectionSaving.value = false
  }
}

const themeOptions = [
  { title: 'Dark', value: 'dark' },
  { title: 'Light', value: 'light' },
  { title: 'System', value: 'system' },
]

const editorThemeOptions = [
  { title: 'Dark', value: 'vs-dark' },
  { title: 'Light', value: 'vs-light' },
  { title: 'High Contrast', value: 'hc-black' },
]

const timeRangeOptions = [
  { title: '5 minutes', value: '5m' },
  { title: '15 minutes', value: '15m' },
  { title: '1 hour', value: '1h' },
  { title: '6 hours', value: '6h' },
  { title: '24 hours', value: '24h' },
]

function handleThemeChange(newTheme: 'light' | 'dark' | 'system'): void {
  settingsStore.updateSetting('theme', newTheme)
  if (newTheme === 'system') {
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
    theme.global.name.value = prefersDark ? 'dark' : 'light'
  } else {
    theme.global.name.value = newTheme
  }
}

function exportSettings(): void {
  const json = settingsStore.exportSettings()
  const blob = new Blob([json], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'varpulis-settings.json'
  a.click()
  URL.revokeObjectURL(url)
}

function openImportDialog(): void {
  importJson.value = ''
  importError.value = null
  importDialog.value = true
}

function importSettings(): void {
  const success = settingsStore.importSettings(importJson.value)
  if (success) {
    importDialog.value = false
    // Apply theme
    handleThemeChange(settingsStore.theme)
  } else {
    importError.value = 'Invalid JSON format'
  }
}

function resetAllSettings(): void {
  settingsStore.resetSettings()
  handleThemeChange(settingsStore.theme)
}

// AI Assistant settings
import { getChatConfig, updateChatConfig } from '@/api/cluster'

const llmForm = ref({
  provider: 'openai-compatible',
  endpoint: '',
  model: '',
  api_key: '',
})
const llmSaving = ref(false)
const llmSaved = ref(false)
const llmError = ref<string | null>(null)
const showLlmKey = ref(false)

const llmProviders = [
  { title: 'Ollama (local)', value: 'openai-compatible', endpoint: 'http://ollama:11434/v1', model: 'qwen2.5:7b' },
  { title: 'OpenAI', value: 'openai-compatible', endpoint: 'https://api.openai.com/v1', model: 'gpt-4o' },
  { title: 'Anthropic', value: 'anthropic', endpoint: 'https://api.anthropic.com/v1', model: 'claude-sonnet-4-5-20250929' },
]

async function loadLlmConfig() {
  try {
    const config = await getChatConfig()
    if (config.configured) {
      llmForm.value.provider = config.provider
      llmForm.value.endpoint = config.endpoint
      llmForm.value.model = config.model
    }
  } catch {
    // Not configured
  }
}

function onProviderSelect(preset: typeof llmProviders[0]) {
  llmForm.value.provider = preset.value
  llmForm.value.endpoint = preset.endpoint
  llmForm.value.model = preset.model
}

async function saveLlmConfig() {
  llmSaving.value = true
  llmError.value = null
  llmSaved.value = false
  try {
    await updateChatConfig({
      endpoint: llmForm.value.endpoint,
      model: llmForm.value.model,
      api_key: llmForm.value.api_key || undefined,
      provider: llmForm.value.provider,
    })
    llmSaved.value = true
    setTimeout(() => { llmSaved.value = false }, 3000)
  } catch (e) {
    llmError.value = e instanceof Error ? e.message : 'Failed to save'
  } finally {
    llmSaving.value = false
  }
}

onMounted(() => {
  loadLlmConfig()
  if (orgStore.currentOrg) {
    loadOrgApiKeys()
    loadOrgMembers()
  }
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Settings</h1>
    </div>

    <v-row>
      <v-col cols="12" md="6">
        <!-- Appearance -->
        <v-card class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-palette</v-icon>
            Appearance
          </v-card-title>
          <v-card-text>
            <v-select
              :model-value="settingsStore.theme"
              label="Theme"
              :items="themeOptions"
              @update:model-value="handleThemeChange"
            />

            <v-select
              v-model="settingsStore.editorTheme"
              label="Editor Theme"
              :items="editorThemeOptions"
              class="mt-4"
            />

            <v-slider
              v-model="settingsStore.editorFontSize"
              label="Editor Font Size"
              :min="10"
              :max="24"
              :step="1"
              thumb-label
              class="mt-4"
            />
          </v-card-text>
        </v-card>

        <!-- Connection -->
        <v-card class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-connection</v-icon>
            Connection
          </v-card-title>
          <v-card-subtitle class="pb-0">
            Current status:
            <v-chip
              v-if="wsStore.apiConnected"
              color="success"
              size="x-small"
              class="ml-1"
            >
              <v-icon start size="x-small">mdi-check-circle</v-icon>
              Connected
            </v-chip>
            <v-chip
              v-else
              color="error"
              size="x-small"
              class="ml-1"
            >
              <v-icon start size="x-small">mdi-alert-circle</v-icon>
              Disconnected
            </v-chip>
          </v-card-subtitle>
          <v-card-text>
            <v-alert
              v-if="hasJwtAuth"
              type="info"
              variant="tonal"
              class="mb-4"
            >
              Authenticated via login session. API key is only needed when connecting to an external coordinator.
            </v-alert>

            <v-alert
              v-if="connectionSaved"
              type="success"
              variant="tonal"
              closable
              class="mb-4"
              @click:close="connectionSaved = false"
            >
              <div class="d-flex align-center">
                <v-icon class="mr-2">mdi-check-circle</v-icon>
                <div>
                  <strong>Connected to coordinator!</strong>
                  <div class="text-body-2">Settings saved and connection verified.</div>
                </div>
              </div>
            </v-alert>

            <v-alert
              v-if="connectionError"
              type="error"
              variant="tonal"
              closable
              class="mb-4"
              @click:close="connectionError = null"
            >
              {{ connectionError }}
            </v-alert>

            <v-text-field
              v-model="connectionForm.coordinatorUrl"
              label="Coordinator URL"
              placeholder="http://localhost:9100"
              hint="Full URL including port. Leave empty to use default proxy."
              persistent-hint
              prepend-inner-icon="mdi-web"
            />

            <v-text-field
              v-model="connectionForm.apiKey"
              label="API Key (optional with login)"
              :type="showApiKey ? 'text' : 'password'"
              :append-inner-icon="showApiKey ? 'mdi-eye-off' : 'mdi-eye'"
              hint="Only needed for external coordinator access or programmatic use"
              persistent-hint
              class="mt-4"
              @click:append-inner="showApiKey = !showApiKey"
            />

            <v-checkbox
              v-model="settingsStore.rememberApiKey"
              label="Remember API key across sessions"
              hint="When enabled, the API key is stored in localStorage. Uncheck for shared machines."
              persistent-hint
              density="compact"
              class="mt-2"
            />

            <div class="d-flex gap-2 mt-4">
              <v-btn
                variant="outlined"
                :loading="connectionTesting"
                prepend-icon="mdi-connection"
                @click="testConnection"
              >
                Test Connection
              </v-btn>
              <v-btn
                color="primary"
                :loading="connectionSaving"
                prepend-icon="mdi-content-save"
                @click="saveConnectionSettings"
              >
                Save & Connect
              </v-btn>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="6">
        <!-- Refresh Settings -->
        <v-card class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-refresh</v-icon>
            Refresh Settings
          </v-card-title>
          <v-card-text>
            <v-slider
              v-model="settingsStore.refreshInterval"
              label="Data Refresh Interval"
              :min="1"
              :max="30"
              :step="1"
              thumb-label
            >
              <template #append>
                <span class="text-body-2">{{ settingsStore.refreshInterval }}s</span>
              </template>
            </v-slider>

            <v-slider
              v-model="settingsStore.workerPollInterval"
              label="Worker Poll Interval"
              :min="1"
              :max="60"
              :step="1"
              thumb-label
              class="mt-4"
            >
              <template #append>
                <span class="text-body-2">{{ settingsStore.workerPollInterval }}s</span>
              </template>
            </v-slider>

            <v-select
              v-model="settingsStore.metricsTimeRange"
              label="Default Metrics Time Range"
              :items="timeRangeOptions"
              class="mt-4"
            />
          </v-card-text>
        </v-card>

        <!-- Notifications -->
        <v-card class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-bell</v-icon>
            Notifications
          </v-card-title>
          <v-card-text>
            <v-switch
              v-model="settingsStore.showNotifications"
              label="Show notifications"
              color="primary"
            />

            <v-switch
              v-model="settingsStore.soundEnabled"
              label="Enable sound"
              color="primary"
              class="mt-2"
            />

            <v-slider
              v-model="settingsStore.maxEventLogSize"
              label="Max Event Log Size"
              :min="100"
              :max="5000"
              :step="100"
              thumb-label
              class="mt-4"
            >
              <template #append>
                <span class="text-body-2">{{ settingsStore.maxEventLogSize }}</span>
              </template>
            </v-slider>
          </v-card-text>
        </v-card>

        <!-- AI Assistant -->
        <v-card class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-robot</v-icon>
            AI Assistant
          </v-card-title>
          <v-card-subtitle>Configure the LLM provider for the chat assistant</v-card-subtitle>
          <v-card-text>
            <v-alert v-if="llmSaved" type="success" variant="tonal" density="compact" class="mb-3">
              Configuration saved successfully
            </v-alert>
            <v-alert v-if="llmError" type="error" variant="tonal" density="compact" class="mb-3">
              {{ llmError }}
            </v-alert>

            <div class="text-subtitle-2 mb-2">Quick Select Provider</div>
            <div class="d-flex gap-2 mb-4">
              <v-chip
                v-for="preset in llmProviders"
                :key="preset.title"
                @click="onProviderSelect(preset)"
                :color="llmForm.endpoint === preset.endpoint ? 'primary' : undefined"
                variant="outlined"
              >
                {{ preset.title }}
              </v-chip>
            </div>

            <v-text-field
              v-model="llmForm.endpoint"
              label="Endpoint URL"
              hint="e.g., http://ollama:11434/v1 or https://api.openai.com/v1"
              persistent-hint
              class="mb-3"
            />
            <v-text-field
              v-model="llmForm.model"
              label="Model Name"
              hint="e.g., qwen2.5:7b, gpt-4o, claude-sonnet-4-5-20250929"
              persistent-hint
              class="mb-3"
            />
            <v-text-field
              v-model="llmForm.api_key"
              label="API Key"
              :type="showLlmKey ? 'text' : 'password'"
              :append-inner-icon="showLlmKey ? 'mdi-eye-off' : 'mdi-eye'"
              @click:append-inner="showLlmKey = !showLlmKey"
              hint="Not needed for Ollama. Required for OpenAI/Anthropic."
              persistent-hint
              class="mb-3"
            />
          </v-card-text>
          <v-card-actions>
            <v-spacer />
            <v-btn
              color="primary"
              :loading="llmSaving"
              :disabled="!llmForm.endpoint || !llmForm.model"
              @click="saveLlmConfig"
            >
              Save
            </v-btn>
          </v-card-actions>
        </v-card>

        <!-- API Keys -->
        <v-card v-if="orgStore.currentOrg" class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-key-variant</v-icon>
            API Keys
          </v-card-title>
          <v-card-subtitle>Manage API keys for {{ orgStore.currentOrg.name }}</v-card-subtitle>
          <v-card-text>
            <!-- New key result -->
            <v-alert v-if="showNewKey && newKeyResult" type="success" variant="tonal" closable class="mb-4" @click:close="showNewKey = false">
              <div class="text-subtitle-2 mb-1">API key created! Copy it now — it won't be shown again.</div>
              <code class="d-block pa-2 bg-surface rounded text-body-2" style="word-break: break-all">{{ newKeyResult }}</code>
            </v-alert>

            <!-- Create new key -->
            <div class="d-flex align-end gap-2 mb-4">
              <v-text-field
                v-model="newKeyName"
                label="Key name"
                density="compact"
                hide-details
                style="max-width: 200px"
              />
              <v-select
                v-model="newKeyExpiresIn"
                :items="expiryOptions"
                label="Expires"
                density="compact"
                hide-details
                style="max-width: 140px"
              />
              <v-btn
                color="primary"
                size="small"
                :loading="newKeyCreating"
                :disabled="!newKeyName.trim()"
                @click="createOrgApiKey"
              >
                Create Key
              </v-btn>
            </div>

            <!-- Key list -->
            <v-table v-if="orgApiKeys.length > 0" density="compact">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Prefix</th>
                  <th>Scopes</th>
                  <th>Created</th>
                  <th>Expires</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="key in orgApiKeys" :key="key.id">
                  <td class="text-body-2">{{ key.name }}</td>
                  <td><code class="text-body-2">{{ key.key_prefix }}...</code></td>
                  <td class="text-body-2">{{ key.scopes }}</td>
                  <td class="text-body-2">{{ new Date(key.created_at).toLocaleDateString() }}</td>
                  <td class="text-body-2">{{ key.expires_at ? new Date(key.expires_at).toLocaleDateString() : 'Never' }}</td>
                  <td>
                    <v-btn
                      v-if="revokeConfirmId !== key.id"
                      icon
                      size="x-small"
                      variant="text"
                      color="error"
                      @click="revokeConfirmId = key.id"
                    >
                      <v-icon size="small">mdi-delete</v-icon>
                      <v-tooltip activator="parent" location="top">Revoke</v-tooltip>
                    </v-btn>
                    <div v-else class="d-flex align-center gap-1">
                      <v-btn size="x-small" color="error" variant="flat" @click="revokeOrgApiKey(key.id)">Revoke</v-btn>
                      <v-btn size="x-small" variant="text" @click="revokeConfirmId = null">Cancel</v-btn>
                    </div>
                  </td>
                </tr>
              </tbody>
            </v-table>
            <div v-else-if="!orgKeysLoading" class="text-medium-emphasis text-body-2">
              No API keys yet. Create one above.
            </div>
            <v-progress-linear v-if="orgKeysLoading" indeterminate class="mt-2" />
          </v-card-text>
        </v-card>

        <!-- Team Members -->
        <v-card v-if="orgStore.currentOrg" class="mb-4">
          <v-card-title>
            <v-icon class="mr-2">mdi-account-group</v-icon>
            Team Members
          </v-card-title>
          <v-card-subtitle>Manage members of {{ orgStore.currentOrg.name }}</v-card-subtitle>
          <v-card-text>
            <!-- Invite form -->
            <div v-if="canManageMembers" class="d-flex align-end gap-2 mb-4">
              <v-text-field
                v-model="inviteEmail"
                label="Email address"
                density="compact"
                hide-details
                style="max-width: 240px"
              />
              <v-select
                v-model="inviteRole"
                :items="roleOptions"
                label="Role"
                density="compact"
                hide-details
                style="max-width: 130px"
              />
              <v-btn
                color="primary"
                size="small"
                :loading="inviting"
                :disabled="!inviteEmail.trim()"
                @click="inviteMember"
              >
                Invite
              </v-btn>
            </div>

            <!-- Members list -->
            <v-table v-if="orgMembers.length > 0" density="compact">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Email</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th v-if="canManageMembers"></th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="member in orgMembers" :key="member.user_id">
                  <td class="text-body-2">{{ member.name || '—' }}</td>
                  <td class="text-body-2">{{ member.email }}</td>
                  <td>
                    <v-select
                      v-if="canManageMembers && member.role !== 'owner'"
                      :model-value="member.role"
                      :items="[...roleOptions, { title: 'Owner', value: 'owner' }]"
                      density="compact"
                      hide-details
                      variant="plain"
                      style="max-width: 120px"
                      @update:model-value="changeMemberRole(member.user_id, $event)"
                    />
                    <v-chip v-else size="x-small" variant="tonal">{{ member.role }}</v-chip>
                  </td>
                  <td>
                    <v-chip :color="member.status === 'active' ? 'success' : 'warning'" size="x-small" variant="tonal">
                      {{ member.status }}
                    </v-chip>
                  </td>
                  <td v-if="canManageMembers">
                    <template v-if="member.role !== 'owner'">
                      <v-btn
                        v-if="removeMemberConfirmId !== member.user_id"
                        icon
                        size="x-small"
                        variant="text"
                        color="error"
                        @click="removeMemberConfirmId = member.user_id"
                      >
                        <v-icon size="small">mdi-account-remove</v-icon>
                        <v-tooltip activator="parent" location="top">Remove</v-tooltip>
                      </v-btn>
                      <div v-else class="d-flex align-center gap-1">
                        <v-btn size="x-small" color="error" variant="flat" @click="removeMember(member.user_id)">Remove</v-btn>
                        <v-btn size="x-small" variant="text" @click="removeMemberConfirmId = null">Cancel</v-btn>
                      </div>
                    </template>
                  </td>
                </tr>
              </tbody>
            </v-table>
            <div v-else-if="!membersLoading" class="text-medium-emphasis text-body-2">
              No team members found.
            </div>
            <v-progress-linear v-if="membersLoading" indeterminate class="mt-2" />
          </v-card-text>
        </v-card>

        <!-- Import/Export -->
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-cog-transfer</v-icon>
            Backup & Restore
          </v-card-title>
          <v-card-text>
            <div class="d-flex gap-2">
              <v-btn
                variant="outlined"
                prepend-icon="mdi-download"
                @click="exportSettings"
              >
                Export Settings
              </v-btn>
              <v-btn
                variant="outlined"
                prepend-icon="mdi-upload"
                @click="openImportDialog"
              >
                Import Settings
              </v-btn>
            </div>

            <v-divider class="my-4" />

            <v-btn
              color="error"
              variant="outlined"
              prepend-icon="mdi-restore"
              @click="resetAllSettings"
            >
              Reset to Defaults
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Import Dialog -->
    <v-dialog v-model="importDialog" max-width="500">
      <v-card>
        <v-card-title>Import Settings</v-card-title>
        <v-card-text>
          <v-textarea
            v-model="importJson"
            label="Settings JSON"
            rows="10"
            :error-messages="importError || undefined"
            class="font-monospace"
          />
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="importDialog = false">Cancel</v-btn>
          <v-btn color="primary" @click="importSettings">Import</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>
