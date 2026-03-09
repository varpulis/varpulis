<script setup lang="ts">
import { useTheme } from 'vuetify'
import { computed } from 'vue'

const theme = useTheme()
const isDark = computed(() => theme.global.current.value.dark)

function toggleTheme() {
  theme.global.name.value = isDark.value ? 'light' : 'dark'
}
</script>

<template>
  <div class="public-layout">
    <v-app-bar flat color="transparent" class="public-nav">
      <v-app-bar-title class="font-weight-bold">
        <router-link to="/landing" class="text-decoration-none" style="color: inherit">
          <v-icon color="primary" class="mr-1">mdi-shield-check</v-icon>
          Varpulis
        </router-link>
      </v-app-bar-title>
      <template #append>
        <v-btn variant="text" to="/scenarios">Demos</v-btn>
        <v-btn variant="text" to="/pricing">Pricing</v-btn>
        <v-btn variant="text" to="/docs">Docs</v-btn>
        <v-btn variant="text" to="/blog">Blog</v-btn>
        <v-btn variant="text" to="/playground">Playground</v-btn>
        <v-btn :icon="isDark ? 'mdi-weather-sunny' : 'mdi-weather-night'" variant="text" @click="toggleTheme" />
        <v-btn variant="outlined" class="ml-2" to="/login">Sign In</v-btn>
        <v-btn color="primary" class="ml-2" to="/signup">Get Started</v-btn>
      </template>
    </v-app-bar>

    <div style="padding-top: 64px">
      <slot />
    </div>

    <v-footer class="py-6">
      <v-container>
        <v-row>
          <v-col cols="12" sm="4">
            <div class="text-subtitle-2 font-weight-bold mb-2">Product</div>
            <div><router-link to="/scenarios" class="text-body-2 text-medium-emphasis text-decoration-none">Demos</router-link></div>
            <div><router-link to="/playground" class="text-body-2 text-medium-emphasis text-decoration-none">Playground</router-link></div>
            <div><router-link to="/pricing" class="text-body-2 text-medium-emphasis text-decoration-none">Pricing</router-link></div>
          </v-col>
          <v-col cols="12" sm="4">
            <div class="text-subtitle-2 font-weight-bold mb-2">Resources</div>
            <div><router-link to="/docs" class="text-body-2 text-medium-emphasis text-decoration-none">Documentation</router-link></div>
            <div><router-link to="/blog" class="text-body-2 text-medium-emphasis text-decoration-none">Blog</router-link></div>
            <div><router-link to="/changelog" class="text-body-2 text-medium-emphasis text-decoration-none">Changelog</router-link></div>
          </v-col>
          <v-col cols="12" sm="4" class="text-sm-right">
            <div class="text-body-2 text-medium-emphasis">&copy; {{ new Date().getFullYear() }} Varpulis</div>
            <div class="text-body-2 text-medium-emphasis">Built with Rust</div>
            <div class="mt-2">
              <v-btn icon size="small" variant="text" href="https://github.com/varpulis/varpulis" target="_blank">
                <v-icon>mdi-github</v-icon>
              </v-btn>
            </div>
          </v-col>
        </v-row>
      </v-container>
    </v-footer>
  </div>
</template>

<style scoped>
.public-layout {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.public-nav {
  backdrop-filter: blur(8px);
}
</style>
