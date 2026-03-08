<script setup lang="ts">
import { useRouter, useRoute } from 'vue-router'
import { useTheme } from 'vuetify'
import { computed } from 'vue'

const router = useRouter()
const route = useRoute()
const theme = useTheme()
const isDark = computed(() => theme.global.current.value.dark)

function toggleTheme() {
  theme.global.name.value = isDark.value ? 'light' : 'dark'
}

// Landing page has its own built-in nav bar
const hasOwnNav = computed(() => route.name === 'landing')

// Scenarios page needs a container wrapper
const wrapInContainer = computed(() => route.name === 'scenarios')
</script>

<template>
  <v-app>
    <!-- Shared nav bar for pages that don't have their own -->
    <v-app-bar v-if="!hasOwnNav" flat density="compact" color="surface">
      <v-app-bar-title class="font-weight-bold" style="cursor: pointer" @click="router.push('/')">
        <v-icon color="primary" size="small" class="mr-1">mdi-shield-check</v-icon>
        Varpulis
      </v-app-bar-title>
      <template #append>
        <v-btn variant="text" size="small" to="/scenarios">Demos</v-btn>
        <v-btn variant="text" size="small" to="/playground">Playground</v-btn>
        <v-btn variant="text" size="small" to="/pricing">Pricing</v-btn>
        <v-btn
          :icon="isDark ? 'mdi-weather-sunny' : 'mdi-weather-night'"
          variant="text"
          size="small"
          @click="toggleTheme"
        />
        <v-btn variant="outlined" size="small" class="ml-2" to="/login">Sign In</v-btn>
        <v-btn color="primary" size="small" class="ml-2" to="/signup">Get Started</v-btn>
      </template>
    </v-app-bar>

    <v-main>
      <template v-if="wrapInContainer">
        <v-container fluid class="pa-4">
          <router-view />
        </v-container>
      </template>
      <template v-else>
        <router-view />
      </template>
    </v-main>
  </v-app>
</template>

<style>
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

.v-table {
  overflow-x: auto;
}
</style>
