<template>
  <v-container class="fill-height" fluid>
    <v-row align="center" justify="center">
      <v-col cols="12" sm="8" md="5">
        <v-card class="pa-6" elevation="8" rounded="lg">
          <v-card-title class="text-center text-h4 font-weight-bold mb-2">
            Email Verification
          </v-card-title>

          <v-divider class="mb-6" />

          <div v-if="loading" class="text-center py-8">
            <v-progress-circular indeterminate color="primary" size="48" />
            <p class="text-body-1 mt-4 text-medium-emphasis">Verifying your email...</p>
          </div>

          <v-alert
            v-if="success"
            type="success"
            variant="tonal"
            class="mb-4"
          >
            {{ success }}
          </v-alert>

          <v-alert
            v-if="error"
            type="error"
            variant="tonal"
            class="mb-4"
          >
            {{ error }}
          </v-alert>

          <div v-if="!loading" class="text-center mt-4">
            <v-btn color="primary" size="large" to="/login">
              Go to Login
            </v-btn>
          </div>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import axios from 'axios'

const route = useRoute()
const loading = ref(true)
const success = ref<string | null>(null)
const error = ref<string | null>(null)

onMounted(async () => {
  const token = route.query.token as string
  if (!token) {
    error.value = 'No verification token provided'
    loading.value = false
    return
  }

  try {
    const baseUrl = import.meta.env.VITE_API_BASE_URL || ''
    const res = await axios.get(`${baseUrl}/auth/verify`, { params: { token } })
    success.value = res.data.message || 'Email verified. You can now log in.'
  } catch (err: any) {
    error.value = err.response?.data?.error || 'Verification failed'
  } finally {
    loading.value = false
  }
})
</script>
