<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  isDark: boolean
  wsConnected: boolean
}>()

const emit = defineEmits<{
  toggleTheme: []
  toggleDrawer: []
}>()

// WebSocket is optional for cluster management (coordinator doesn't have WS)
const connectionColor = computed(() => (props.wsConnected ? 'success' : 'grey'))
const connectionText = computed(() => (props.wsConnected ? 'Live' : 'Offline'))
</script>

<template>
  <v-app-bar elevation="1" density="comfortable">
    <v-app-bar-nav-icon @click="emit('toggleDrawer')" />

    <v-toolbar-title class="d-flex align-center">
      <v-icon class="mr-2" color="primary">mdi-lightning-bolt</v-icon>
      <span class="font-weight-bold">Varpulis</span>
      <span class="text-medium-emphasis ml-2">Control Plane</span>
    </v-toolbar-title>

    <v-spacer />

    <!-- Connection Status -->
    <v-chip
      :color="connectionColor"
      variant="flat"
      size="small"
      class="mr-3"
    >
      <v-icon start size="small">
        {{ wsConnected ? 'mdi-wifi' : 'mdi-wifi-off' }}
      </v-icon>
      {{ connectionText }}
    </v-chip>

    <!-- Theme Toggle -->
    <v-btn
      icon
      variant="text"
      @click="emit('toggleTheme')"
    >
      <v-icon>{{ isDark ? 'mdi-weather-sunny' : 'mdi-weather-night' }}</v-icon>
      <v-tooltip activator="parent" location="bottom">
        {{ isDark ? 'Light mode' : 'Dark mode' }}
      </v-tooltip>
    </v-btn>

    <!-- Documentation Link -->
    <v-btn
      icon
      variant="text"
      href="https://github.com/your-org/varpulis"
      target="_blank"
    >
      <v-icon>mdi-github</v-icon>
      <v-tooltip activator="parent" location="bottom">
        Documentation
      </v-tooltip>
    </v-btn>
  </v-app-bar>
</template>
