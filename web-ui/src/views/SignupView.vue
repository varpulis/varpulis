<template>
  <v-container class="fill-height" fluid>
    <v-row align="center" justify="center">
      <v-col cols="12" sm="8" md="5">
        <v-card class="pa-6" elevation="8" rounded="lg">
          <v-card-title class="text-center text-h4 font-weight-bold mb-2">
            Create Account
          </v-card-title>

          <v-card-subtitle class="text-center text-body-1 mb-6">
            Start your free trial — no credit card required
          </v-card-subtitle>

          <v-divider class="mb-6" />

          <v-alert
            v-if="success"
            type="success"
            variant="tonal"
            class="mb-4"
          >
            {{ success }}
            <template #append>
              <v-btn variant="text" size="small" to="/login">Go to Login</v-btn>
            </template>
          </v-alert>

          <v-alert
            v-if="error"
            type="error"
            variant="tonal"
            class="mb-4"
            closable
            @click:close="error = null"
          >
            {{ error }}
          </v-alert>

          <v-form v-if="!success" @submit.prevent="handleRegister">
            <v-text-field
              v-model="orgName"
              label="Organization Name"
              variant="outlined"
              density="comfortable"
              class="mb-2"
              :disabled="loading"
            />
            <v-text-field
              v-model="username"
              label="Username"
              variant="outlined"
              density="comfortable"
              class="mb-2"
              :disabled="loading"
              autocomplete="username"
            />
            <v-text-field
              v-model="email"
              label="Email"
              type="email"
              variant="outlined"
              density="comfortable"
              class="mb-2"
              :disabled="loading"
              autocomplete="email"
            />
            <v-text-field
              v-model="password"
              label="Password"
              variant="outlined"
              density="comfortable"
              type="password"
              class="mb-2"
              :disabled="loading"
              autocomplete="new-password"
              hint="At least 8 characters"
            />
            <v-text-field
              v-model="confirmPassword"
              label="Confirm Password"
              variant="outlined"
              density="comfortable"
              type="password"
              class="mb-4"
              :disabled="loading"
              autocomplete="new-password"
              :error-messages="confirmError"
            />
            <v-btn
              color="primary"
              size="large"
              block
              type="submit"
              :loading="loading"
              :disabled="!canSubmit"
            >
              Create Account
            </v-btn>
          </v-form>

          <v-card-text class="text-center text-caption mt-4 text-medium-emphasis">
            Already have an account?
            <router-link to="/login" class="text-primary">Sign in</router-link>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import axios from 'axios'

const orgName = ref('')
const username = ref('')
const email = ref('')
const password = ref('')
const confirmPassword = ref('')
const loading = ref(false)
const error = ref<string | null>(null)
const success = ref<string | null>(null)

const confirmError = computed(() => {
  if (confirmPassword.value && password.value !== confirmPassword.value) {
    return 'Passwords do not match'
  }
  return ''
})

const canSubmit = computed(() => {
  return (
    username.value &&
    email.value &&
    password.value.length >= 8 &&
    password.value === confirmPassword.value &&
    !loading.value
  )
})

async function handleRegister() {
  if (!canSubmit.value) return

  loading.value = true
  error.value = null

  try {
    const baseUrl = import.meta.env.VITE_API_BASE_URL || ''
    const res = await axios.post(`${baseUrl}/auth/register`, {
      username: username.value,
      email: email.value,
      password: password.value,
      org_name: orgName.value,
    })
    success.value = res.data.message || 'Check your email to verify your account'
  } catch (err: any) {
    error.value = err.response?.data?.error || 'Registration failed'
  } finally {
    loading.value = false
  }
}
</script>
