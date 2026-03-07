<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useTheme } from 'vuetify'
import AppBar from '@/components/common/AppBar.vue'
import NavDrawer from '@/components/common/NavDrawer.vue'
import ChatPanel from '@/components/chat/ChatPanel.vue'
import { useWebSocketStore } from '@/stores/websocket'
import { useAuthStore } from '@/stores/auth'
import { getApiKey, setApiKey } from '@/api'
import { getChatConfig } from '@/api/cluster'

const theme = useTheme()
const router = useRouter()
const route = useRoute()
const wsStore = useWebSocketStore()
const authStore = useAuthStore()

const drawer = ref(true)
const rail = ref(false)

const isDark = computed(() => theme.global.current.value.dark)
const isLoginPage = computed(() => route.name === 'login')

// Start health checks and validate auth on mount
onMounted(() => {
  const hasToken = localStorage.getItem('varpulis_token')
  const sessionAuth = localStorage.getItem('varpulis_authenticated')
  const apiKey = getApiKey()

  // Auto-populate from build-time env var (for demo deployments)
  if (!apiKey && import.meta.env.VITE_API_KEY) {
    setApiKey(import.meta.env.VITE_API_KEY)
  }

  if (hasToken || sessionAuth || apiKey || import.meta.env.VITE_API_KEY) {
    // Validate existing auth and start health checks
    authStore.handleOAuthCallback()
    wsStore.startHealthCheck(10000)
  }
})

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
  { title: 'Admin', icon: 'mdi-shield-crown', to: '/admin' },
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
    <template v-if="!isLoginPage">
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
    </template>

    <v-main>
      <v-container fluid class="pa-4">
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
      :aria-label="chatDrawer ? 'Close AI assistant' : 'Open AI assistant'"
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
      role="status"
      aria-live="polite"
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
