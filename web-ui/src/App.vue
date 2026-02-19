<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useTheme } from 'vuetify'
import AppBar from '@/components/common/AppBar.vue'
import NavDrawer from '@/components/common/NavDrawer.vue'
import ChatPanel from '@/components/chat/ChatPanel.vue'
import { useWebSocketStore } from '@/stores/websocket'
import { useSettingsStore } from '@/stores/settings'
import { getApiKey, setApiKey } from '@/api'
import { getChatConfig } from '@/api/cluster'

const theme = useTheme()
const router = useRouter()
const route = useRoute()
const wsStore = useWebSocketStore()
const settingsStore = useSettingsStore()

const drawer = ref(true)
const rail = ref(false)
const showApiKeyBanner = ref(false)
const apiKeyInput = ref('')

const isDark = computed(() => theme.global.current.value.dark)

// Check for API key on mount and start health check
onMounted(() => {
  let existingKey = getApiKey() || settingsStore.apiKey
  // Auto-populate from build-time env var (for demo deployments)
  if (!existingKey && import.meta.env.VITE_API_KEY) {
    setApiKey(import.meta.env.VITE_API_KEY)
    existingKey = import.meta.env.VITE_API_KEY
  }
  if (!existingKey) {
    showApiKeyBanner.value = true
  } else {
    // Start API health check to update connection status
    wsStore.startHealthCheck(10000)
  }
})

function saveApiKey(): void {
  if (apiKeyInput.value.trim()) {
    setApiKey(apiKeyInput.value.trim())
    settingsStore.updateSetting('apiKey', apiKeyInput.value.trim())
    showApiKeyBanner.value = false
    // Reload the page to apply the new API key
    window.location.reload()
  }
}

function dismissBanner(): void {
  showApiKeyBanner.value = false
}

function useDevKey(): void {
  apiKeyInput.value = 'dev-key'
  saveApiKey()
}

function toggleTheme() {
  theme.global.name.value = isDark.value ? 'light' : 'dark'
}

function toggleDrawer() {
  if (drawer.value && !rail.value) {
    rail.value = true
  } else if (drawer.value && rail.value) {
    drawer.value = false
    rail.value = false
  } else {
    drawer.value = true
    rail.value = false
  }
}

const navItems = [
  { title: 'Dashboard', icon: 'mdi-view-dashboard', to: '/' },
  { title: 'Cluster', icon: 'mdi-server-network', to: '/cluster' },
  { title: 'Connectors', icon: 'mdi-connection', to: '/connectors' },
  { title: 'Pipelines', icon: 'mdi-pipe', to: '/pipelines' },
  { title: 'Monitoring', icon: 'mdi-monitor-dashboard', to: '/monitoring' },
  { title: 'Editor', icon: 'mdi-code-braces', to: '/editor' },
  { title: 'Demos', icon: 'mdi-presentation-play', to: '/scenarios' },
  { title: 'Models', icon: 'mdi-brain', to: '/models' },
  { title: 'Metrics', icon: 'mdi-chart-line', to: '/metrics' },
  { title: 'Settings', icon: 'mdi-cog', to: '/settings' },
]

const chatDrawer = ref(false)
const chatConfigured = ref(false)

async function checkChatConfig() {
  try {
    const config = await getChatConfig()
    chatConfigured.value = config.configured
  } catch {
    chatConfigured.value = false
  }
}

onMounted(() => {
  checkChatConfig()
})

// WebSocket connection is optional for cluster management
// Only connect if we have an API key and want real-time updates
// The coordinator doesn't have a WebSocket, so this is disabled by default
// wsStore.connect()
</script>

<template>
  <v-app>
    <AppBar
      :is-dark="isDark"
      :ws-connected="wsStore.connected"
      @toggle-theme="toggleTheme"
      @toggle-drawer="toggleDrawer"
    />

    <NavDrawer
      v-model="drawer"
      :rail="rail"
      :items="navItems"
      :current-route="route.path"
      @navigate="router.push($event)"
    />

    <v-main>
      <v-container fluid class="pa-4">
        <!-- API Key Setup Banner -->
        <v-alert
          v-if="showApiKeyBanner"
          type="warning"
          variant="tonal"
          closable
          class="mb-4"
          @click:close="dismissBanner"
        >
          <div class="d-flex flex-column gap-2">
            <div>
              <strong>API Key Required</strong>
              <div class="text-body-2">Enter your coordinator API key to connect to the cluster.</div>
              <div class="text-caption text-medium-emphasis mt-1">
                For development, use: <code class="font-weight-bold">dev-key</code>
              </div>
            </div>
            <div class="d-flex align-center gap-2">
              <v-text-field
                v-model="apiKeyInput"
                label="API Key"
                density="compact"
                variant="outlined"
                hide-details
                style="max-width: 300px"
                @keyup.enter="saveApiKey"
              />
              <v-btn color="primary" @click="saveApiKey">Save</v-btn>
              <v-btn variant="text" @click="useDevKey">Use Dev Key</v-btn>
            </div>
          </div>
        </v-alert>

        <router-view v-slot="{ Component, route }">
          <transition name="fade" mode="out-in">
            <keep-alive :include="['EditorView']">
              <component :is="Component" :key="route.path" />
            </keep-alive>
          </transition>
        </router-view>
      </v-container>
    </v-main>

    <!-- Chat FAB -->
    <v-btn
      icon
      color="primary"
      size="large"
      class="chat-fab"
      :disabled="!chatConfigured"
      @click="chatDrawer = !chatDrawer"
    >
      <v-icon>{{ chatDrawer ? 'mdi-close' : 'mdi-robot' }}</v-icon>
      <v-tooltip v-if="!chatConfigured" activator="parent" location="start">
        Configure an LLM provider in Settings
      </v-tooltip>
    </v-btn>

    <!-- Chat Drawer -->
    <v-navigation-drawer
      v-model="chatDrawer"
      location="right"
      width="420"
      temporary
    >
      <ChatPanel />
    </v-navigation-drawer>

    <v-snackbar
      v-model="wsStore.showConnectionSnackbar"
      :color="wsStore.connected ? 'success' : 'error'"
      :timeout="3000"
    >
      {{ wsStore.connected ? 'Connected to server' : 'Disconnected from server' }}
    </v-snackbar>
  </v-app>
</template>

<style>
.chat-fab {
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 1000;
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.2s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}

/* Scrollbar styling for dark theme */
::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

::-webkit-scrollbar-track {
  background: transparent;
}

::-webkit-scrollbar-thumb {
  background: rgba(128, 128, 128, 0.5);
  border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
  background: rgba(128, 128, 128, 0.7);
}
</style>
