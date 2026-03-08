<template>
  <v-container class="fill-height" fluid>
    <v-row align="center" justify="center">
      <v-col cols="12" sm="8" md="4">
        <v-card class="pa-6" elevation="8" rounded="lg">
          <v-card-title class="text-center text-h4 font-weight-bold mb-2">
            Varpulis
          </v-card-title>

          <v-card-subtitle class="text-center text-body-1 mb-6">
            Sign in to access the control plane
          </v-card-subtitle>

          <v-divider class="mb-6" />

          <v-alert
            v-if="authStore.error"
            type="error"
            variant="tonal"
            class="mb-4"
            closable
            @click:close="authStore.error = null"
          >
            {{ authStore.error }}
          </v-alert>

          <!-- Username/Password Form -->
          <v-form @submit.prevent="handleLogin">
            <v-text-field
              v-model="username"
              label="Username"
              variant="outlined"
              density="comfortable"
              class="mb-2"
              :disabled="authStore.loading"
              autocomplete="username"
            />
            <v-text-field
              v-model="password"
              label="Password"
              variant="outlined"
              density="comfortable"
              type="password"
              class="mb-4"
              :disabled="authStore.loading"
              autocomplete="current-password"
              @keyup.enter="handleLogin"
            />
            <v-btn
              color="primary"
              size="large"
              block
              type="submit"
              :loading="authStore.loading"
              :disabled="!username || !password"
            >
              Sign in
            </v-btn>
          </v-form>

          <div class="d-flex align-center my-4">
            <v-divider />
            <span class="mx-3 text-caption text-medium-emphasis">or</span>
            <v-divider />
          </div>

          <!-- GitHub OAuth Button -->
          <v-btn
            color="grey-darken-4"
            size="large"
            block
            variant="outlined"
            :loading="authStore.loading"
            @click="authStore.loginWithGitHub()"
          >
            <template #prepend>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="20"
                height="20"
                viewBox="0 0 24 24"
                fill="currentColor"
              >
                <path
                  d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207
                  11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729
                  1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304
                  3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931
                  0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0
                  1.008-.322 3.301 1.23.957-.266 1.983-.399
                  3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552
                  3.297-1.23 3.297-1.23.653 1.653.242 2.874.118
                  3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807
                  5.624-5.479 5.921.43.372.823 1.102.823
                  2.222v3.293c0 .319.192.694.801.576 4.765-1.589
                  8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"
                />
              </svg>
            </template>
            Sign in with GitHub
          </v-btn>

          <v-card-text class="text-center text-caption mt-4 text-medium-emphasis">
            By signing in, you agree to the terms of service.
          </v-card-text>

          <v-card-text class="text-center text-body-2 pt-0">
            Don't have an account?
            <router-link to="/signup" class="text-primary">Create one</router-link>
          </v-card-text>

          <v-card-text class="text-center text-caption pt-0">
            <router-link to="/landing" class="text-medium-emphasis">&larr; Back to home</router-link>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const username = ref('')
const password = ref('')

async function handleLogin() {
  if (username.value && password.value) {
    await authStore.loginWithPassword(username.value, password.value)
  }
}
</script>
